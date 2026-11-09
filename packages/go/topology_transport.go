package sirannon

import (
	"context"
	"sync"
)

type topologyTransport struct {
	client        *TopologyClient
	id            string
	mutex         sync.Mutex
	closed        bool
	pool          map[string]databaseTransport
	subscriptions *topologySubscriptions
}

func newTopologyTransport(client *TopologyClient, id string) *topologyTransport {
	transport := &topologyTransport{client: client, id: id, pool: map[string]databaseTransport{}}
	transport.subscriptions = newTopologySubscriptions(transport)
	return transport
}

func (t *topologyTransport) endpoint(url string) (databaseTransport, error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.closed {
		return nil, transportClosed()
	}
	if existing := t.pool[url]; existing != nil {
		return existing, nil
	}
	var created databaseTransport
	if t.client.settings.transport == TransportHTTP {
		created = newHTTPDatabase(t.client.server(url).http, t.id)
	} else {
		created = newWSTransport(webSocketBase(url)+"/db/"+escapePathSegment(t.id), t.client.settings, t.client.lifetime)
	}
	t.pool[url] = created
	return created, nil
}

func (t *topologyTransport) readOn(url string, read func(databaseTransport) error) error {
	transport, err := t.endpoint(url)
	if err != nil {
		return err
	}
	return read(transport)
}

func (t *topologyTransport) onRead(ctx context.Context, level ReadConcernLevel, read func(databaseTransport) error) error {
	client := t.client
	url, err := client.readEndpoint(ctx, t.id, level)
	if err != nil {
		return err
	}
	err = t.readOn(url, read)
	if err == nil || ctx.Err() != nil {
		return err
	}
	unreachable := endpointUnreachable(err)
	if client.coordinated() && shouldRefreshRouting(err) {
		if unreachable {
			client.setAside(url)
		}
		if refreshErr := client.refreshRouting(ctx, t.id); refreshErr != nil {
			return refreshErr
		}
		retryURL, routeErr := client.readEndpoint(ctx, t.id, level)
		if routeErr != nil {
			return routeErr
		}
		return t.readOn(retryURL, read)
	}
	writeURL, writeErr := client.writeEndpoint(ctx, t.id)
	if writeErr != nil || !unreachable || url == writeURL {
		return err
	}
	client.setAside(url)
	fallbackURL, routeErr := client.readEndpoint(ctx, t.id, level)
	if routeErr != nil {
		return routeErr
	}
	return t.readOn(fallbackURL, read)
}

func (t *topologyTransport) onWrite(ctx context.Context, write func(databaseTransport) error) error {
	url, err := t.client.writeEndpoint(ctx, t.id)
	if err != nil {
		return err
	}
	transport, err := t.endpoint(url)
	if err != nil {
		return err
	}
	err = write(transport)
	if err != nil && ctx.Err() == nil && t.client.coordinated() && shouldRefreshRouting(err) {
		_ = t.client.refreshRouting(ctx, t.id)
	}
	return err
}

func perQueryLevel(call statementCall) ReadConcernLevel {
	if call.readConcern == nil {
		return ""
	}
	return call.readConcern.Level
}

func (t *topologyTransport) query(ctx context.Context, call statementCall) ([]Row, error) {
	level := perQueryLevel(call)
	var rows []Row
	err := t.onRead(ctx, level, func(transport databaseTransport) error {
		var err error
		rows, err = transport.query(ctx, call)
		return err
	})
	return rows, err
}

func (t *topologyTransport) execute(ctx context.Context, call statementCall) ([]ExecuteResult, error) {
	var results []ExecuteResult
	err := t.onWrite(ctx, func(transport databaseTransport) error {
		var err error
		results, err = transport.execute(ctx, call)
		return err
	})
	return results, err
}

func (t *topologyTransport) transaction(ctx context.Context, statements []Statement, concern *WriteConcern) ([]ExecuteResult, error) {
	var results []ExecuteResult
	err := t.onWrite(ctx, func(transport databaseTransport) error {
		var err error
		results, err = transport.transaction(ctx, statements, concern)
		return err
	})
	return results, err
}

func (t *topologyTransport) batch(ctx context.Context, sql string, paramsBatch []any, concern *WriteConcern) ([]ExecuteResult, error) {
	var results []ExecuteResult
	err := t.onWrite(ctx, func(transport databaseTransport) error {
		var err error
		results, err = transport.batch(ctx, sql, paramsBatch, concern)
		return err
	})
	return results, err
}

func (t *topologyTransport) load(ctx context.Context, sql string, paramsBatch []any, options LoadOptions) (LoadResult, error) {
	var result LoadResult
	err := t.onWrite(ctx, func(transport databaseTransport) error {
		var err error
		result, err = transport.load(ctx, sql, paramsBatch, options)
		return err
	})
	return result, err
}

func (t *topologyTransport) ack(ctx context.Context, deviceID string, seq int64) (*AckResponse, error) {
	var answer *AckResponse
	err := t.onWrite(ctx, func(transport databaseTransport) error {
		var err error
		answer, err = transport.ack(ctx, deviceID, seq)
		return err
	})
	return answer, err
}

func (t *topologyTransport) subscribe(ctx context.Context, spec subscriptionSpec) (closer, error) {
	open := func(ctx context.Context, transport databaseTransport) (closer, error) {
		return transport.subscribe(ctx, spec)
	}
	return t.subscriptions.track(ctx, open)
}

func (t *topologyTransport) live(ctx context.Context, spec liveSpec) (closer, error) {
	open := func(ctx context.Context, transport databaseTransport) (closer, error) {
		return transport.live(ctx, spec)
	}
	return t.subscriptions.track(ctx, open)
}

func (t *topologyTransport) routingChanged(ctx context.Context) error {
	return t.subscriptions.migrate(ctx)
}

func (t *topologyTransport) close() {
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		return
	}
	t.closed = true
	pool := t.pool
	t.pool = map[string]databaseTransport{}
	t.subscriptions.forgetLocked()
	t.mutex.Unlock()
	for _, transport := range pool {
		transport.close()
	}
}

func (c *TopologyClient) Database(id string) (*RemoteDatabase, error) {
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
	transport := newTopologyTransport(c, id)
	var database *RemoteDatabase
	database = newRemoteDatabase(c.lifetime, id, transport, func(ctx context.Context) (*capabilityCache, error) {
		return c.capabilitiesFor(ctx, id)
	}, func() {
		c.mutex.Lock()
		if c.databases[id] == database {
			delete(c.databases, id)
			delete(c.transports, id)
		}
		c.mutex.Unlock()
	})
	c.databases[id] = database
	c.transports[id] = transport
	return database, nil
}

func (c *TopologyClient) Close() {
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
	c.transports = map[string]*topologyTransport{}
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
