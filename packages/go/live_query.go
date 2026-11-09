package sirannon

import (
	"context"
	"maps"
	"slices"
	"sync"
)

// LiveStatus is the status of a [LiveQuery]: pending, ready, or failed.
type LiveStatus string

const (
	// LivePending means that the client is waiting for the first result.
	LivePending LiveStatus = "pending"
	// LiveReady means that [LiveState.Rows] holds the current result.
	LiveReady LiveStatus = "ready"
	// LiveFailed means that the live query failed with the error in [LiveState.Err], and Rows is
	// empty.
	LiveFailed LiveStatus = "error"
)

// LiveUpdateKind is the kind of change in a [LiveUpdate].
type LiveUpdateKind string

const (
	// LiveUpdateRows means that the client replaced the rows with a full result, as it does with the
	// first result and after a reconnection.
	LiveUpdateRows LiveUpdateKind = "rows"
	// LiveUpdateOps means that the client applied the edits in [LiveUpdate.Ops] to the rows.
	LiveUpdateOps LiveUpdateKind = "ops"
	// LiveUpdateRevalidating means that the connection dropped, so the client keeps the rows as they
	// were until it receives a fresh result.
	LiveUpdateRevalidating LiveUpdateKind = "revalidating"
	// LiveUpdateError means that the live query failed, and [LiveState.Err] holds the error.
	LiveUpdateError LiveUpdateKind = "error"
)

// LiveState is the state of a [LiveQuery] at one point in time.
type LiveState[R any] struct {
	// Status is pending, ready, or failed.
	Status LiveStatus
	// Rows holds the current rows, in the order in which the read returns them.
	Rows []R
	// Revalidating is true while the connection is down, which means that Rows can be out of date.
	Revalidating bool
	// Err holds the failure while Status is [LiveFailed].
	Err error
}

// LiveOp is one edit to the rows of a [LiveQuery].
type LiveOp[R any] struct {
	// Op is insert, update, or delete.
	Op string
	// Index is the position in the rows where the client applies the edit, counted after every
	// earlier edit of the same update.
	Index int
	// Row holds the new row for an insert or an update, and the zero value of R for a delete.
	Row R
}

// LiveUpdate is what a listener of a [LiveQuery] receives each time the rows change.
type LiveUpdate[R any] struct {
	// Kind is the kind of change.
	Kind LiveUpdateKind
	// Ops holds the edits of a [LiveUpdateOps] update in order, and nil for every other kind.
	Ops []LiveOp[R]
	// State holds the state after the update. Each listener receives a separate copy of the rows, so
	// a listener can keep them.
	State LiveState[R]
}

// LiveQuery keeps the rows of a registered read current as the table changes, from the moment when
// [Live] opens it until [LiveQuery.Close] stops it. Many goroutines can call one LiveQuery at once.
type LiveQuery[R any] struct {
	convert   func(Row) (R, error)
	onError   func(error)
	events    *dispatcher
	stop      context.CancelFunc
	mutex     sync.Mutex
	state     LiveState[R]
	listeners map[uint64]func(LiveUpdate[R])
	nextID    uint64
	handle    closer
	closed    bool
}

func openLiveQuery[R any](ctx context.Context, db *RemoteDatabase, name string, args map[string]any, convert func(Row) (R, error), onError func(error)) (*LiveQuery[R], error) {
	lifetime, stop := context.WithCancel(db.lifetime)
	query := &LiveQuery[R]{
		convert:   convert,
		onError:   onError,
		events:    startDispatcher(lifetime),
		stop:      stop,
		state:     LiveState[R]{Status: LivePending},
		listeners: map[uint64]func(LiveUpdate[R]){},
	}
	handle, err := db.openLive(ctx, name, args, query)
	if err != nil {
		query.shut()
		return nil, err
	}
	query.mutex.Lock()
	query.handle = handle
	failure := query.state.Err
	query.mutex.Unlock()
	if failure != nil {
		_ = query.Close(ctx)
		return nil, failure
	}
	return query, nil
}

// State returns the current state, with a copy of the rows that you can keep.
func (q *LiveQuery[R]) State() LiveState[R] {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.snapshotLocked()
}

func (q *LiveQuery[R]) snapshotLocked() LiveState[R] {
	snapshot := q.state
	snapshot.Rows = slices.Clone(q.state.Rows)
	return snapshot
}

// Subscribe adds a listener for every later update and returns a function that removes it. The
// client calls the listeners one at a time, in the order of the updates, on a goroutine
// apart from the one that reads the connection. When a listener panics, the client recovers and
// passes a [*CallbackPanic] to the [OnError] handler that you gave to [Live]. Subscribe ignores a nil
// listener and every listener that you add after Close.
func (q *LiveQuery[R]) Subscribe(listener func(LiveUpdate[R])) func() {
	if listener == nil {
		return func() {}
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return func() {}
	}
	q.nextID++
	id := q.nextID
	q.listeners[id] = listener
	return func() {
		q.mutex.Lock()
		delete(q.listeners, id)
		q.mutex.Unlock()
	}
}

// Close removes every listener and tells the server to stop sending updates. A second Close returns
// nil.
func (q *LiveQuery[R]) Close(ctx context.Context) error {
	q.mutex.Lock()
	if q.closed {
		q.mutex.Unlock()
		return nil
	}
	handle := q.handle
	q.handle = nil
	q.mutex.Unlock()
	q.shut()
	if handle == nil {
		return nil
	}
	return handle.unsubscribe(ctx)
}

func (q *LiveQuery[R]) shut() {
	q.mutex.Lock()
	q.closed = true
	q.listeners = map[uint64]func(LiveUpdate[R]){}
	q.mutex.Unlock()
	q.stop()
}

func (q *LiveQuery[R]) convertRows(rows []Row) ([]R, error) {
	converted := make([]R, 0, len(rows))
	for _, row := range rows {
		value, err := q.convert(row)
		if err != nil {
			return nil, err
		}
		converted = append(converted, value)
	}
	return converted, nil
}

func (q *LiveQuery[R]) rows(rows []Row) {
	converted, err := q.convertRows(rows)
	if err != nil {
		q.fail(err)
		return
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return
	}
	q.publishLocked(LiveState[R]{Status: LiveReady, Rows: converted}, LiveUpdate[R]{Kind: LiveUpdateRows})
}

func (q *LiveQuery[R]) ops(ops []liveOp) {
	typed := make([]LiveOp[R], 0, len(ops))
	for _, op := range ops {
		entry := LiveOp[R]{Op: op.op, Index: op.index}
		if op.op != "delete" {
			value, err := q.convert(op.row)
			if err != nil {
				q.fail(err)
				return
			}
			entry.Row = value
		}
		typed = append(typed, entry)
	}
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return
	}
	if q.state.Status != LiveReady {
		q.failLocked(invalidResponse("A live query received row operations before its first result"))
		return
	}
	next := slices.Clone(q.state.Rows)
	for _, op := range typed {
		highest := len(next) - 1
		if op.Op == "insert" {
			highest = len(next)
		}
		if op.Index < 0 || op.Index > highest {
			q.failLocked(invalidResponse("A live query received an operation at index %d, outside its %d rows", op.Index, len(next)))
			return
		}
		switch op.Op {
		case "insert":
			next = slices.Insert(next, op.Index, op.Row)
		case "update":
			next[op.Index] = op.Row
		default:
			next = slices.Delete(next, op.Index, op.Index+1)
		}
	}
	q.publishLocked(LiveState[R]{Status: LiveReady, Rows: next}, LiveUpdate[R]{Kind: LiveUpdateOps, Ops: typed})
}

func (q *LiveQuery[R]) pace(ctx context.Context) {
	q.events.paced(ctx)
}

func (q *LiveQuery[R]) revalidating() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed || q.state.Status != LiveReady || q.state.Revalidating {
		return
	}
	q.publishLocked(LiveState[R]{Status: LiveReady, Rows: q.state.Rows, Revalidating: true}, LiveUpdate[R]{Kind: LiveUpdateRevalidating})
}

func (q *LiveQuery[R]) fail(failure error) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.closed {
		return
	}
	q.failLocked(failure)
}

func (q *LiveQuery[R]) failLocked(failure error) {
	q.publishLocked(LiveState[R]{Status: LiveFailed, Err: failure}, LiveUpdate[R]{Kind: LiveUpdateError})
}

func (q *LiveQuery[R]) publishLocked(next LiveState[R], update LiveUpdate[R]) {
	q.state = next
	if len(q.listeners) == 0 {
		return
	}
	for _, id := range slices.Sorted(maps.Keys(q.listeners)) {
		listener := q.listeners[id]
		delivered := update
		delivered.State = q.snapshotLocked()
		delivered.Ops = slices.Clone(update.Ops)
		onError := q.onError
		q.events.post(func() { invokeCallback(onError, func() { listener(delivered) }) })
	}
}
