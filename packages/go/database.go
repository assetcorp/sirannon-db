package sirannon

import (
	"context"
	"iter"
	"sync"
)

const defaultLoadBatchSize = 1000

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

func (d *RemoteDatabase) Ack(ctx context.Context, deviceID string, seq int64) (*AckResponse, error) {
	var answer *AckResponse
	err := d.run(ctx, func(ctx context.Context) error {
		var err error
		answer, err = d.transport.ack(ctx, deviceID, seq)
		return err
	})
	return answer, err
}

func (d *RemoteDatabase) On(table string) *SubscriptionBuilder {
	return &SubscriptionBuilder{database: d, table: table}
}

func (d *RemoteDatabase) Close() {
	d.closeOnce.Do(func() {
		d.cancel()
		d.transport.close()
		if d.onClose != nil {
			d.onClose()
		}
	})
}
