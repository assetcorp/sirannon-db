package sirannon

import (
	"context"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	clusterDiscoveryTimeout = 2 * time.Second
	latencyCacheWindow      = 60 * time.Second
	unreachableSetAside     = 5 * time.Second
	latencyProbeTimeout     = 5 * time.Second
)

var routingRefreshCodes = []string{"STALE_PRIMARY", "AUTHORITY_LOST", "COORDINATOR_UNAVAILABLE", CodeNoSafePrimary, CodeConnectionError}

type readEndpoint struct {
	url      string
	concerns []ReadConcernLevel
}

type clusterRouting struct {
	primary string
	term    string
	reads   []readEndpoint
}

func shouldRefreshRouting(err error) bool {
	return slices.Contains(routingRefreshCodes, ErrorCode(err))
}

func endpointUnreachable(err error) bool {
	code := ErrorCode(err)
	return code == CodeConnectionError || code == CodeTimeout
}

func routingFingerprint(routing *clusterRouting) string {
	if routing == nil {
		return ""
	}
	entries := make([]string, 0, len(routing.reads))
	for _, endpoint := range routing.reads {
		concerns := make([]string, 0, len(endpoint.concerns))
		for _, concern := range endpoint.concerns {
			concerns = append(concerns, string(concern))
		}
		slices.Sort(concerns)
		entries = append(entries, endpoint.url+"\x1f"+strings.Join(concerns, ","))
	}
	slices.Sort(entries)
	return routing.primary + "\x1e" + routing.term + "\x1e" + strings.Join(entries, "\x1d")
}

func discoveredServerBase(endpoint, databaseID string) (string, error) {
	base, err := normaliseServerURL(endpoint, "A discovered endpoint")
	if err != nil {
		return "", invalidResponse("Cluster metadata holds an endpoint that the client refuses to use: %s", ErrorCode(err))
	}
	suffix := regexp.MustCompile(`(?i)/db/` + regexp.QuoteMeta(escapePathSegment(databaseID)) + `$`)
	return suffix.ReplaceAllString(base, ""), nil
}

func validReadConcern(level ReadConcernLevel) bool {
	return level == ReadConcernLevelLocal || level == ReadConcernLevelMajority || level == ReadConcernLevelLinearizable
}

func parseClusterRouting(data any, databaseID string) (*clusterRouting, error) {
	record, isObject := data.(map[string]any)
	if !isObject {
		return nil, invalidResponse("Cluster metadata must be an object")
	}
	if id, _ := record["databaseId"].(string); id != databaseID {
		return nil, invalidResponse("Cluster metadata describes another database")
	}
	routing := &clusterRouting{}
	if term, present := record["primaryTerm"]; present {
		text, isText := term.(string)
		if !isText {
			return nil, invalidResponse("Cluster metadata primaryTerm must be a string")
		}
		routing.term = text
	}
	if primary := record["currentPrimary"]; primary != nil {
		fields, isFields := primary.(map[string]any)
		if !isFields {
			return nil, invalidResponse("Cluster metadata currentPrimary must be an object or null")
		}
		endpoint, isText := fields["endpoint"].(string)
		if !isText {
			return nil, invalidResponse("Cluster metadata currentPrimary.endpoint must be a string")
		}
		base, err := discoveredServerBase(endpoint, databaseID)
		if err != nil {
			return nil, err
		}
		routing.primary = base
	}
	reads, present := record["readEndpoints"]
	if !present {
		return routing, nil
	}
	list, isList := reads.([]any)
	if !isList {
		return nil, invalidResponse("Cluster metadata readEndpoints must be an array")
	}
	for _, entry := range list {
		fields, isFields := entry.(map[string]any)
		if !isFields {
			return nil, invalidResponse("Cluster metadata holds a read endpoint that is not an object")
		}
		endpoint, isText := fields["endpoint"].(string)
		if !isText {
			return nil, invalidResponse("Cluster metadata holds a read endpoint whose URL is not a string")
		}
		base, err := discoveredServerBase(endpoint, databaseID)
		if err != nil {
			return nil, err
		}
		concerns, isConcerns := fields["readConcerns"].([]any)
		if !isConcerns {
			return nil, invalidResponse("Cluster metadata readConcerns must be an array")
		}
		parsed := readEndpoint{url: base}
		for _, concern := range concerns {
			text, _ := concern.(string)
			level := ReadConcernLevel(text)
			if !validReadConcern(level) {
				return nil, invalidResponse("Cluster metadata holds a read concern that the client does not know")
			}
			if !slices.Contains(parsed.concerns, level) {
				parsed.concerns = append(parsed.concerns, level)
			}
		}
		routing.reads = append(routing.reads, parsed)
	}
	return routing, nil
}

func (c *TopologyClient) fetchClusterRouting(ctx context.Context, base, databaseID string) (*clusterRouting, error) {
	server := c.server(base)
	answer, err := server.http.exchange(ctx, exchange{method: http.MethodGet, path: "/db/" + escapePathSegment(databaseID) + "/cluster"}, []CallOption{WithTimeout(clusterDiscoveryTimeout)})
	if err != nil {
		if ctx.Err() != nil || ErrorCode(err) == CodeTransportError {
			return nil, err
		}
		return nil, nil
	}
	var data any
	if decodeWire(answer.body, &data) != nil {
		return nil, nil
	}
	return parseClusterRouting(data, databaseID)
}

func (c *TopologyClient) discoveryCandidates(databaseID string) []string {
	var candidates []string
	add := func(url string) {
		if url != "" && !slices.Contains(candidates, url) {
			candidates = append(candidates, url)
		}
	}
	for _, endpoint := range c.endpoints {
		add(endpoint)
	}
	add(c.primary)
	for _, replica := range c.replicas {
		add(replica)
	}
	c.mutex.Lock()
	existing := c.routing[databaseID]
	c.mutex.Unlock()
	if existing != nil {
		add(existing.primary)
		for _, endpoint := range existing.reads {
			add(endpoint.url)
		}
	}
	add(c.base)
	return candidates
}

func (c *TopologyClient) refreshRouting(ctx context.Context, databaseID string) error {
	gate := c.refreshGate(databaseID)
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return contextFailure(ctx.Err(), "the cluster routing of "+databaseID)
	}
	defer func() { <-gate }()
	for _, candidate := range c.discoveryCandidates(databaseID) {
		next, err := c.fetchClusterRouting(ctx, candidate, databaseID)
		if err != nil {
			return err
		}
		if next == nil {
			continue
		}
		c.mutex.Lock()
		previous := c.routing[databaseID]
		c.routing[databaseID] = next
		transport := c.transports[databaseID]
		c.mutex.Unlock()
		if previous != nil && routingFingerprint(previous) == routingFingerprint(next) {
			return nil
		}
		if transport == nil {
			return nil
		}
		if err := transport.routingChanged(ctx); err != nil {
			c.mutex.Lock()
			if previous != nil {
				c.routing[databaseID] = previous
			} else {
				delete(c.routing, databaseID)
			}
			c.mutex.Unlock()
			return err
		}
		return nil
	}
	return newError(CodeRoutingError, "No endpoint answered with the cluster routing of database "+databaseID)
}

func (c *TopologyClient) refreshGate(databaseID string) chan struct{} {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	gate := c.refreshing[databaseID]
	if gate == nil {
		gate = make(chan struct{}, 1)
		c.refreshing[databaseID] = gate
	}
	return gate
}

func (c *TopologyClient) ensureRouting(ctx context.Context, databaseID string) (*clusterRouting, error) {
	c.mutex.Lock()
	existing := c.routing[databaseID]
	c.mutex.Unlock()
	if existing != nil {
		return existing, nil
	}
	if err := c.refreshRouting(ctx, databaseID); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if refreshed := c.routing[databaseID]; refreshed != nil {
		return refreshed, nil
	}
	return nil, newError(CodeRoutingError, "No endpoint answered with the cluster routing of database "+databaseID)
}

func (c *TopologyClient) setAside(url string) {
	c.mutex.Lock()
	c.aside[url] = time.Now().Add(unreachableSetAside)
	c.mutex.Unlock()
}

func (c *TopologyClient) reachable(url string) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	until, found := c.aside[url]
	if !found {
		return true
	}
	if time.Now().Before(until) {
		return false
	}
	delete(c.aside, url)
	return true
}
