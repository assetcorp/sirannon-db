package sirannon

import (
	"context"
	"iter"
	"sync"
)

const defaultLoadBatchSize = 1000

// RemoteDatabase sends statements, registered operations, subscriptions, and live queries to one
// database on the server. [Client.Database] and [TopologyClient.Database] return it. Many goroutines
// can call one RemoteDatabase at once.
type RemoteDatabase struct {
	id        string
	transport databaseTransport
	sqlGate   func(ctx context.Context) (*capabilityCache, error)
	lifetime  context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	onClose   func()
}

func newRemoteDatabase(parent context.Context, id string, transport databaseTransport, sqlGate func(context.Context) (*capabilityCache, error), onClose func()) *RemoteDatabase {
	lifetime, cancel := context.WithCancel(parent)
	return &RemoteDatabase{id: id, transport: transport, sqlGate: sqlGate, lifetime: lifetime, cancel: cancel, onClose: onClose}
}

// ID returns the identifier of the database.
func (d *RemoteDatabase) ID() string {
	return d.id
}

func (d *RemoteDatabase) run(ctx context.Context, work func(ctx context.Context) error) error {
	if d.lifetime.Err() != nil {
		return transportClosed()
	}
	scoped, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(d.lifetime, cancel)
	defer stop()
	err := work(scoped)
	if err != nil && d.lifetime.Err() != nil {
		return transportClosed()
	}
	return err
}

func (d *RemoteDatabase) requireSQL(ctx context.Context) error {
	cache, err := d.sqlGate(ctx)
	if err != nil {
		return err
	}
	return cache.requireSQL(ctx, d.lifetime.Done())
}

func (d *RemoteDatabase) digest(ctx context.Context, refresh bool) (string, error) {
	cache, err := d.sqlGate(ctx)
	if err != nil {
		return "", err
	}
	return cache.registryDigest(ctx, refresh, d.lifetime.Done())
}

// Query sends one read statement and returns its rows. The params argument takes a []any of
// positional values, a map[string]any of named values, or nil. When the server refuses SQL
// statements, Query fails with SQL_NOT_ACCEPTED before the client sends the statement.
func (d *RemoteDatabase) Query(ctx context.Context, sql string, params any, options ...QueryOption) ([]Row, error) {
	var rows []Row
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		var err error
		rows, err = d.transport.query(ctx, statementCall{sql: sql, params: params, readConcern: readQueryOptions(options).readConcern})
		return err
	})
	return rows, err
}

// Execute sends one write statement and returns what it changed. It takes params in the same forms
// as [RemoteDatabase.Query]. Like Query, it fails with SQL_NOT_ACCEPTED before sending anything when
// the server refuses SQL statements.
func (d *RemoteDatabase) Execute(ctx context.Context, sql string, params any) (ExecuteResult, error) {
	var result ExecuteResult
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		results, err := d.transport.execute(ctx, statementCall{sql: sql, params: params})
		if err != nil {
			return err
		}
		if len(results) != 1 {
			return invalidResponse("The server answered one statement with %d results", len(results))
		}
		result = results[0]
		return nil
	})
	return result, err
}

// Transaction sends the statements as one transaction, so either all of them take effect or none
// does. It returns one result for each statement in order, or INVALID_ARGUMENT for an empty list.
func (d *RemoteDatabase) Transaction(ctx context.Context, statements []Statement, options ...ExecuteOption) ([]ExecuteResult, error) {
	if len(statements) == 0 {
		return nil, invalidArgument("A transaction needs at least one statement")
	}
	var results []ExecuteResult
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		var err error
		results, err = d.transport.transaction(ctx, statements, readExecuteOptions(options).writeConcern)
		return err
	})
	return results, err
}

// Batch sends one statement with every parameter set in paramsBatch, which the server commits in one
// transaction. It returns one result for each parameter set, or INVALID_ARGUMENT for an empty
// paramsBatch.
func (d *RemoteDatabase) Batch(ctx context.Context, sql string, paramsBatch []any, options ...ExecuteOption) ([]ExecuteResult, error) {
	var results []ExecuteResult
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		var err error
		results, err = d.transport.batch(ctx, sql, paramsBatch, readExecuteOptions(options).writeConcern)
		return err
	})
	return results, err
}

// Load sends one statement with every parameter set in paramsBatch and returns the totals. The server
// relaxes the writer's durability during the load and restores the configured durability before it
// answers. If the server crashes during a load, SQLite rolls back the uncommitted rows, so you can
// send the same load again.
func (d *RemoteDatabase) Load(ctx context.Context, sql string, paramsBatch []any, options LoadOptions) (LoadResult, error) {
	var result LoadResult
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		var err error
		result, err = d.transport.load(ctx, sql, paramsBatch, options)
		return err
	})
	return result, err
}

// LoadAll sends one statement with every parameter set that rows yields, in requests of
// [LoadAllOptions.BatchSize] rows. The server checkpoints the WAL only after the last request,
// because LoadAll sets [LoadOptions.Checkpoint] to false on every earlier one. When a request fails,
// LoadAll stops and returns the totals of the requests before it together with the error.
func (d *RemoteDatabase) LoadAll(ctx context.Context, sql string, rows iter.Seq[any], options LoadAllOptions) (LoadResult, error) {
	batchSize := options.BatchSize
	if batchSize < 0 {
		return LoadResult{}, invalidArgument("LoadAllOptions.BatchSize must be a positive integer")
	}
	if batchSize == 0 {
		batchSize = defaultLoadBatchSize
	}
	var total LoadResult
	err := d.run(ctx, func(ctx context.Context) error {
		if err := d.requireSQL(ctx); err != nil {
			return err
		}
		send := func(batch []any, checkpoint bool) error {
			summary, err := d.transport.load(ctx, sql, batch, LoadOptions{Durability: options.Durability, Checkpoint: Ptr(checkpoint)})
			total.RowsLoaded += summary.RowsLoaded
			total.Changes += summary.Changes
			return err
		}
		var previous, current []any
		for row := range rows {
			current = append(current, row)
			if len(current) < batchSize {
				continue
			}
			if previous != nil {
				if err := send(previous, false); err != nil {
					return err
				}
			}
			previous, current = current, nil
		}
		switch {
		case len(current) > 0 && previous != nil:
			if err := send(previous, false); err != nil {
				return err
			}
			return send(current, true)
		case len(current) > 0:
			return send(current, true)
		case previous != nil:
			return send(previous, true)
		}
		return nil
	})
	return total, err
}

func (d *RemoteDatabase) queryOperation(ctx context.Context, name string, args map[string]any, options []QueryOption) ([]Row, error) {
	var rows []Row
	err := d.run(ctx, func(ctx context.Context) error {
		var err error
		rows, err = d.transport.query(ctx, statementCall{name: name, args: args, readConcern: readQueryOptions(options).readConcern})
		return err
	})
	return rows, err
}

func (d *RemoteDatabase) executeOperation(ctx context.Context, name string, args map[string]any, options []ExecuteOption) ([]ExecuteResult, error) {
	var results []ExecuteResult
	err := d.run(ctx, func(ctx context.Context) error {
		var err error
		results, err = d.transport.execute(ctx, statementCall{name: name, args: args, writeConcern: readExecuteOptions(options).writeConcern})
		return err
	})
	return results, err
}

func (d *RemoteDatabase) openLive(ctx context.Context, name string, args map[string]any, handlers liveHandlers) (closer, error) {
	var handle closer
	err := d.run(ctx, func(ctx context.Context) error {
		var err error
		handle, err = d.transport.live(ctx, liveSpec{name: name, args: args, handlers: handlers, digest: d.digest})
		return err
	})
	return handle, err
}

// Ack tells the server that the device with the identifier deviceID has stored every change up to
// seq. It needs the WebSocket transport, so it fails with TRANSPORT_ERROR over HTTP.
func (d *RemoteDatabase) Ack(ctx context.Context, deviceID string, seq int64) (*AckResponse, error) {
	var answer *AckResponse
	err := d.run(ctx, func(ctx context.Context) error {
		var err error
		answer, err = d.transport.ack(ctx, deviceID, seq)
		return err
	})
	return answer, err
}

// On returns a [SubscriptionBuilder] for the changes to table.
func (d *RemoteDatabase) On(table string) *SubscriptionBuilder {
	return &SubscriptionBuilder{database: d, table: table}
}

// Close closes the database's connection, so every later call on it fails with TRANSPORT_ERROR. A
// later call to Database with the same identifier returns a new RemoteDatabase. A second call to
// Close returns at once.
func (d *RemoteDatabase) Close() {
	d.closeOnce.Do(func() {
		d.cancel()
		d.transport.close()
		if d.onClose != nil {
			d.onClose()
		}
	})
}
