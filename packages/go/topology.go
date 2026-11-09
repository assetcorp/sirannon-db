package sirannon

import (
	"context"
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"
)

// ReadPreference sets which node of a replication group serves a read.
type ReadPreference string

const (
	// ReadFromPrimary makes the client send every read to the primary. The client uses it unless you
	// set another.
	ReadFromPrimary ReadPreference = "primary"
	// ReadFromReplica makes the client send each read to a reachable replica that it picks at random,
	// or to the primary when no replica is reachable.
	ReadFromReplica ReadPreference = "replica"
	// ReadFromNearest makes the client send each read to the reachable node with the lowest latency.
	// Under
	// [DiscoveryStatic], the client measures each node's latency with GET /health and keeps the
	// measurements for 60 seconds. Under [DiscoveryCoordinator], the client takes the first reachable
	// endpoint that the cluster routing lists for the read concern.
	ReadFromNearest ReadPreference = "nearest"
)

// Discovery sets how the client finds the addresses of the nodes in a replication group.
type Discovery string

const (
	// DiscoveryStatic makes the client use the Primary and Replicas that you set. The client uses it
	// unless you set another.
	DiscoveryStatic Discovery = "static"
	// DiscoveryCoordinator makes the client fetch the routing of each database from
	// GET /db/{id}/cluster and fetch it again when a call fails because the primary moved or a node
	// stopped answering.
	DiscoveryCoordinator Discovery = "coordinator"
)

// TopologyOptions holds the settings of a [TopologyClient]. NewTopology fails with INVALID_ARGUMENT
// unless you set at least one of Primary, Replicas, or Endpoints.
type TopologyOptions struct {
	// Options holds the transport settings that the client applies to every node.
	Options
	// Endpoints lists the nodes that the client asks first for the cluster routing under
	// [DiscoveryCoordinator].
	Endpoints []string
	// Primary is the address of the node that takes writes under [DiscoveryStatic]. When you leave
	// it unset, the client writes to the first address in Replicas, or else in Endpoints.
	Primary string
	// Replicas lists the addresses of the nodes that serve reads under [ReadFromReplica] and
	// [ReadFromNearest].
	Replicas []string
	// ReadPreference sets the node for each read. The client uses [ReadFromPrimary] unless you set
	// another.
	ReadPreference ReadPreference
	// Discovery sets how the client finds the nodes. The client uses [DiscoveryStatic] unless you set
	// another.
	Discovery Discovery
	// ReadConcern sets, under [DiscoveryCoordinator], which nodes may serve a read: the nodes that the
	// cluster routing lists for this level, or the primary for a linearizable read. The client uses
	// majority unless you set another level, but it ignores ReadConcern under [DiscoveryStatic].
	ReadConcern ReadConcernLevel
}

type serverEndpoint struct {
	http         *httpTransport
	capabilities *capabilityCache
}

type endpointLatency struct {
	url       string
	latency   time.Duration
	reachable bool
}

type latencyProbe struct {
	done chan struct{}
}

// TopologyClient sends the writes of each database to the primary of a replication group and sends
// its reads to the node that matches [TopologyOptions.ReadPreference]. [NewTopology] returns it. Many
// goroutines can call one TopologyClient at once.
type TopologyClient struct {
	settings    *settings
	lifetime    context.Context
	cancel      context.CancelFunc
	tasks       *taskGroup
	primary     string
	replicas    []string
	endpoints   []string
	base        string
	preference  ReadPreference
	discovery   Discovery
	readConcern ReadConcernLevel

	mutex      sync.Mutex
	servers    map[string]*serverEndpoint
	routing    map[string]*clusterRouting
	refreshing map[string]chan struct{}
	aside      map[string]time.Time
	latencies  []endpointLatency
	measuredAt time.Time
	measuring  *latencyProbe
	databases  map[string]*RemoteDatabase
	transports map[string]*topologyTransport
	closed     bool
}

func normaliseList(urls []string, label string) ([]string, error) {
	normalised := make([]string, 0, len(urls))
	for _, raw := range urls {
		base, err := normaliseServerURL(raw, label)
		if err != nil {
			return nil, err
		}
		normalised = append(normalised, base)
	}
	return normalised, nil
}

// NewTopology checks the options and returns a TopologyClient, without opening a connection. It
// accepts only absolute http or https addresses with no credentials, query string, or fragment, so
// any other address fails with INVALID_ARGUMENT, as does an option outside its range.
func NewTopology(options TopologyOptions) (*TopologyClient, error) {
	resolved, err := resolveSettings(options.Options)
	if err != nil {
		return nil, err
	}
	client := &TopologyClient{
		settings:    resolved,
		tasks:       newTaskGroup(),
		preference:  options.ReadPreference,
		discovery:   options.Discovery,
		readConcern: options.ReadConcern,
		servers:     map[string]*serverEndpoint{},
		routing:     map[string]*clusterRouting{},
		refreshing:  map[string]chan struct{}{},
		aside:       map[string]time.Time{},
		databases:   map[string]*RemoteDatabase{},
		transports:  map[string]*topologyTransport{},
	}
	if client.preference == "" {
		client.preference = ReadFromPrimary
	}
	if client.discovery == "" {
		client.discovery = DiscoveryStatic
	}
	if !slices.Contains([]ReadPreference{ReadFromPrimary, ReadFromReplica, ReadFromNearest}, client.preference) {
		return nil, invalidArgument("TopologyOptions.ReadPreference takes primary, replica, or nearest")
	}
	if client.discovery != DiscoveryStatic && client.discovery != DiscoveryCoordinator {
		return nil, invalidArgument("TopologyOptions.Discovery takes static or coordinator")
	}
	if client.readConcern != "" && !validReadConcern(client.readConcern) {
		return nil, invalidArgument("TopologyOptions.ReadConcern takes local, majority, or linearizable")
	}
	if options.Primary != "" {
		if client.primary, err = normaliseServerURL(options.Primary, "TopologyOptions.Primary"); err != nil {
			return nil, err
		}
	}
	if client.replicas, err = normaliseList(options.Replicas, "Each of TopologyOptions.Replicas"); err != nil {
		return nil, err
	}
	if client.endpoints, err = normaliseList(options.Endpoints, "Each of TopologyOptions.Endpoints"); err != nil {
		return nil, err
	}
	for _, candidate := range append(append([]string{client.primary}, client.replicas...), client.endpoints...) {
		if candidate != "" {
			client.base = candidate
			break
		}
	}
	if client.base == "" {
		return nil, invalidArgument("A topology client needs a primary, a replica, or a starter endpoint")
	}
	client.lifetime, client.cancel = context.WithCancel(context.Background())
	return client, nil
}

func (c *TopologyClient) server(base string) *serverEndpoint {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if existing := c.servers[base]; existing != nil {
		return existing
	}
	transport := newHTTPTransport(base, c.settings, c.lifetime)
	created := &serverEndpoint{http: transport, capabilities: newCapabilityCache(transport, c.tasks)}
	c.servers[base] = created
	return created
}

func (c *TopologyClient) coordinated() bool {
	return c.discovery == DiscoveryCoordinator
}

func (c *TopologyClient) staticPrimary() string {
	if c.primary != "" {
		return c.primary
	}
	return c.base
}

func (c *TopologyClient) writeEndpoint(ctx context.Context, databaseID string) (string, error) {
	if !c.coordinated() {
		return c.staticPrimary(), nil
	}
	routing, err := c.ensureRouting(ctx, databaseID)
	if err != nil {
		return "", err
	}
	if routing.primary == "" {
		return "", newError(CodeNoSafePrimary, "The cluster names no current primary for database "+databaseID)
	}
	return routing.primary, nil
}

func (c *TopologyClient) readEndpoint(ctx context.Context, databaseID string, level ReadConcernLevel) (string, error) {
	if c.coordinated() {
		return c.routedReadEndpoint(ctx, databaseID, level)
	}
	switch c.preference {
	case ReadFromReplica:
		var available []string
		for _, replica := range c.replicas {
			if c.reachable(replica) {
				available = append(available, replica)
			}
		}
		if len(available) == 0 {
			return c.staticPrimary(), nil
		}
		return available[rand.IntN(len(available))], nil
	case ReadFromNearest:
		measured, err := c.measuredLatencies(ctx)
		if err != nil {
			return "", err
		}
		best, bestLatency := "", time.Duration(math.MaxInt64)
		for _, entry := range measured {
			if entry.reachable && c.reachable(entry.url) && entry.latency < bestLatency {
				best, bestLatency = entry.url, entry.latency
			}
		}
		if best == "" {
			return c.staticPrimary(), nil
		}
		return best, nil
	}
	return c.staticPrimary(), nil
}

func (c *TopologyClient) routedReadEndpoint(ctx context.Context, databaseID string, level ReadConcernLevel) (string, error) {
	routing, err := c.ensureRouting(ctx, databaseID)
	if err != nil {
		return "", err
	}
	if level == "" {
		level = c.readConcern
	}
	if level == "" {
		level = ReadConcernLevelMajority
	}
	if level == ReadConcernLevelLinearizable {
		if routing.primary == "" {
			return "", newError(CodeNoSafePrimary, "The cluster names no current primary for a linearizable read")
		}
		return routing.primary, nil
	}
	var readable []string
	for _, endpoint := range routing.reads {
		if slices.Contains(endpoint.concerns, level) && c.reachable(endpoint.url) {
			if c.preference == ReadFromReplica && routing.primary != "" && endpoint.url == routing.primary {
				continue
			}
			readable = append(readable, endpoint.url)
		}
	}
	if c.preference != ReadFromPrimary && len(readable) > 0 {
		if c.preference == ReadFromNearest {
			return readable[0], nil
		}
		return readable[rand.IntN(len(readable))], nil
	}
	if routing.primary != "" {
		return routing.primary, nil
	}
	for _, endpoint := range routing.reads {
		if slices.Contains(endpoint.concerns, ReadConcernLevelLocal) {
			return endpoint.url, nil
		}
	}
	return "", newError(CodeRoutingError, "No usable read endpoint is available for database "+databaseID)
}

func (c *TopologyClient) capabilitiesFor(ctx context.Context, databaseID string) (*capabilityCache, error) {
	if !c.coordinated() {
		return c.server(c.staticPrimary()).capabilities, nil
	}
	routing, err := c.ensureRouting(ctx, databaseID)
	if err != nil {
		return nil, err
	}
	target := routing.primary
	if target == "" {
		if target, err = c.routedReadEndpoint(ctx, databaseID, ""); err != nil {
			return nil, err
		}
	}
	return c.server(target).capabilities, nil
}

func (c *TopologyClient) measuredLatencies(ctx context.Context) ([]endpointLatency, error) {
	c.mutex.Lock()
	if len(c.latencies) > 0 && time.Since(c.measuredAt) < latencyCacheWindow {
		measured := c.latencies
		c.mutex.Unlock()
		return measured, nil
	}
	probe := c.measuring
	if probe == nil {
		probe = &latencyProbe{done: make(chan struct{})}
		if !c.tasks.start(func() { c.measure(probe) }) {
			c.mutex.Unlock()
			return nil, transportClosed()
		}
		c.measuring = probe
	}
	c.mutex.Unlock()
	select {
	case <-probe.done:
	case <-ctx.Done():
		return nil, contextFailure(ctx.Err(), "the latency probes")
	case <-c.lifetime.Done():
		return nil, transportClosed()
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.latencies, nil
}

func (c *TopologyClient) measure(probe *latencyProbe) {
	targets := slices.Clone(c.replicas)
	if c.primary != "" {
		targets = append([]string{c.primary}, targets...)
	}
	measured := make([]endpointLatency, len(targets))
	var group sync.WaitGroup
	for position, target := range targets {
		group.Go(func() {
			measured[position] = c.probeLatency(target)
		})
	}
	group.Wait()
	c.mutex.Lock()
	c.latencies, c.measuredAt, c.measuring = measured, time.Now(), nil
	c.mutex.Unlock()
	close(probe.done)
}

func (c *TopologyClient) probeLatency(target string) endpointLatency {
	started := time.Now()
	_, err := c.server(target).http.exchange(c.lifetime, exchange{method: http.MethodGet, path: "/health"}, []CallOption{WithTimeout(latencyProbeTimeout)})
	if err != nil {
		return endpointLatency{url: target, latency: time.Duration(math.MaxInt64)}
	}
	return endpointLatency{url: target, latency: time.Since(started), reachable: true}
}
