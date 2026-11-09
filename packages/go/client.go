package sirannon

import (
	"context"
	"net/url"
	"strings"
	"sync"
)

type Operations struct {
	transport    *httpTransport
	capabilities *capabilityCache
}

func (o *Operations) requireSQL(ctx context.Context) error {
	return o.capabilities.requireSQL(ctx, o.transport.lifetime.Done())
}

type Client struct {
	*Operations
	base      string
	settings  *settings
	lifetime  context.Context
	cancel    context.CancelFunc
	tasks     *taskGroup
	mutex     sync.Mutex
	databases map[string]*RemoteDatabase
	closed    bool
}

func normaliseServerURL(raw string, label string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return "", invalidArgument("%s must be an absolute http or https URL", label)
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", invalidArgument("%s must use http or https", label)
	}
	if parsed.User != nil {
		return "", invalidArgument("%s must not carry credentials; pass them in Options.Headers", label)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || strings.Contains(raw, "#") {
		return "", invalidArgument("%s must not carry a query string or a fragment", label)
	}
	parsed.Scheme = scheme
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")
	return strings.TrimRight(parsed.String(), "/"), nil
}

func webSocketBase(base string) string {
	if rest, found := strings.CutPrefix(base, "https://"); found {
		return "wss://" + rest
	}
	return "ws://" + strings.TrimPrefix(base, "http://")
}

func New(serverURL string, options Options) (*Client, error) {
	base, err := normaliseServerURL(serverURL, "The server URL")
	if err != nil {
		return nil, err
	}
	resolved, err := resolveSettings(options)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	tasks := newTaskGroup()
	transport := newHTTPTransport(base, resolved, lifetime)
	return &Client{
		Operations: &Operations{transport: transport, capabilities: newCapabilityCache(transport, tasks)},
		base:       base,
		settings:   resolved,
		lifetime:   lifetime,
		cancel:     cancel,
		tasks:      tasks,
		databases:  map[string]*RemoteDatabase{},
	}, nil
}

func (c *Client) Database(id string) (*RemoteDatabase, error) {
	if id == "" {
		return nil, invalidArgument("A database needs an identifier")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return nil, transportClosed()
	}
	if existing := c.databases[id]; existing != nil {
		return existing, nil
	}
	var transport databaseTransport
	if c.settings.transport == TransportHTTP {
		transport = newHTTPDatabase(c.transport, id)
	} else {
		transport = newWSTransport(webSocketBase(c.base)+"/db/"+escapePathSegment(id), c.settings, c.lifetime)
	}
	capabilities := c.capabilities
	var database *RemoteDatabase
	database = newRemoteDatabase(c.lifetime, id, transport, func(context.Context) (*capabilityCache, error) {
		return capabilities, nil
	}, func() {
		c.mutex.Lock()
		if c.databases[id] == database {
			delete(c.databases, id)
		}
		c.mutex.Unlock()
	})
	c.databases[id] = database
	return database, nil
}

func (c *Client) Close() {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return
	}
	c.closed = true
	open := make([]*RemoteDatabase, 0, len(c.databases))
	for _, database := range c.databases {
		open = append(open, database)
	}
	c.databases = map[string]*RemoteDatabase{}
	c.mutex.Unlock()
	for _, database := range open {
		database.Close()
	}
	c.cancel()
	c.tasks.closeAndWait()
	if c.settings.ownsHTTPClient {
		c.settings.httpClient.CloseIdleConnections()
	}
}
