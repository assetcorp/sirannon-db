package sirannon

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
)

type serverFrame struct {
	Type         string            `json:"type"`
	ID           string            `json:"id"`
	Data         json.RawMessage   `json:"data"`
	Error        json.RawMessage   `json:"error"`
	Event        json.RawMessage   `json:"event"`
	Events       []json.RawMessage `json:"events"`
	Seq          *string           `json:"seq"`
	Epoch        *string           `json:"epoch"`
	Resync       bool              `json:"resync"`
	Rows         json.RawMessage   `json:"rows"`
	Ops          json.RawMessage   `json:"ops"`
	Revalidating bool              `json:"revalidating"`
}

type liveOp struct {
	op    string
	index int
	row   Row
}

type liveHandlers interface {
	rows(rows []Row)
	ops(ops []liveOp)
	revalidating()
	fail(failure error)
	pace(ctx context.Context)
}

func (t *wsTransport) paceCallbacks() {
	t.events.paced(t.lifetime)
	t.mutex.Lock()
	handlers := make([]liveHandlers, 0, len(t.lives))
	for _, live := range t.lives {
		handlers = append(handlers, live.spec.handlers)
	}
	t.mutex.Unlock()
	for _, handler := range handlers {
		handler.pace(t.lifetime)
	}
}

func webSocketReadLimit(maxResponseBytes int64) int64 {
	if maxResponseBytes == 0 {
		return -1
	}
	return maxResponseBytes
}

func present(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

func (t *wsTransport) route(data []byte) {
	var frame serverFrame
	if json.Unmarshal(data, &frame) != nil {
		return
	}
	switch frame.Type {
	case "result":
		t.settle(frame.ID, frame.Data, nil)
	case "unsubscribed":
		t.settle(frame.ID, nil, nil)
	case "subscribed":
		t.subscribed(frame)
	case "error":
		failure := remoteFrameError(frame.Error)
		if !t.settle(frame.ID, nil, failure) {
			t.unclaimedError(frame.ID, failure)
		}
	case "change":
		t.deliver(frame.ID, []json.RawMessage{frame.Event})
	case "changes":
		t.deliver(frame.ID, frame.Events)
	case "live":
		t.liveMessage(frame)
	}
}

func (t *wsTransport) unclaimedError(id string, failure *Error) {
	t.mutex.Lock()
	subscription := t.subscriptions[id]
	live := t.lives[id]
	t.mutex.Unlock()
	if live != nil {
		live.spec.handlers.fail(failure)
	}
	if subscription != nil {
		onError := subscription.spec.onError
		t.events.post(func() { reportFailure(onError, failure) })
	}
}

func (t *wsTransport) subscribed(frame serverFrame) {
	t.mutex.Lock()
	subscription := t.subscriptions[frame.ID]
	live := t.lives[frame.ID]
	if subscription != nil {
		t.applySubscribedLocked(subscription, frame)
	}
	t.mutex.Unlock()
	if live != nil {
		rows, err := decodeRows(frame.Rows)
		if err != nil {
			live.spec.handlers.fail(err)
		} else {
			live.spec.handlers.rows(rows)
		}
	}
	t.settle(frame.ID, nil, nil)
}

func (t *wsTransport) applySubscribedLocked(subscription *activeSubscription, frame serverFrame) {
	subscription.established = true
	if frame.Epoch != nil {
		subscription.epoch = *frame.Epoch
		subscription.hasEpoch = true
	}
	baseline, baselineErr := int64(0), error(nil)
	hasBaseline := frame.Seq != nil
	if hasBaseline {
		baseline, baselineErr = strconv.ParseInt(*frame.Seq, 10, 64)
		hasBaseline = baselineErr == nil
	}
	spec := subscription.spec
	if frame.Resync {
		subscription.lastSeq, subscription.hasSeq = baseline, hasBaseline
		if spec.onReset != nil {
			t.events.post(func() { invokeCallback(spec.onError, spec.onReset) })
		}
		return
	}
	if !subscription.hasSeq && hasBaseline {
		subscription.lastSeq, subscription.hasSeq = baseline, true
	}
}

func decodeRows(raw json.RawMessage) ([]Row, error) {
	if !present(raw) {
		return []Row{}, nil
	}
	var rows []map[string]any
	if err := decodeWire(raw, &rows); err != nil {
		return nil, invalidResponse("The server sent rows that the client cannot decode: %v", err)
	}
	return rowsOf(rows), nil
}

func changeEventOf(raw json.RawMessage) (ChangeEvent, error) {
	var wire WSWireChangeEvent
	if err := decodeWire(raw, &wire); err != nil {
		return ChangeEvent{}, invalidResponse("The server sent a change that the client cannot decode: %v", err)
	}
	seq, err := strconv.ParseInt(wire.Seq, 10, 64)
	if err != nil {
		return ChangeEvent{}, invalidResponse("The server sent a change whose seq %q is not a 64-bit integer", wire.Seq)
	}
	event := ChangeEvent{
		Type:      string(wire.Type),
		Table:     wire.Table,
		Row:       wire.Row,
		OldRow:    wire.OldRow,
		Seq:       seq,
		Timestamp: wire.Timestamp,
		HLC:       textOf(wire.HLC),
		Origin:    textOf(wire.Origin),
		RowID:     textOf(wire.RowID),
		TxID:      textOf(wire.TxID),
		TxEnd:     wire.TxEnd != nil && *wire.TxEnd,
	}
	if event.Row == nil {
		event.Row = Row{}
	}
	return event, nil
}

func textOf(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func (t *wsTransport) deliver(id string, raws []json.RawMessage) {
	for _, raw := range raws {
		event, err := changeEventOf(raw)
		t.mutex.Lock()
		subscription := t.subscriptions[id]
		if subscription == nil {
			t.mutex.Unlock()
			return
		}
		spec := subscription.spec
		if err != nil {
			t.mutex.Unlock()
			t.events.post(func() { reportFailure(spec.onError, err) })
			continue
		}
		if subscription.hasSeq && event.Seq <= subscription.lastSeq {
			t.mutex.Unlock()
			continue
		}
		subscription.lastSeq, subscription.hasSeq = event.Seq, true
		t.mutex.Unlock()
		t.events.post(func() { invokeCallback(spec.onError, func() { spec.callback(event) }) })
	}
}

func liveOpsOf(raw json.RawMessage) ([]liveOp, error) {
	var wire []WSLiveOp
	if err := decodeWire(raw, &wire); err != nil {
		return nil, invalidResponse("The server sent live operations that the client cannot decode: %v", err)
	}
	ops := make([]liveOp, 0, len(wire))
	for _, entry := range wire {
		var op liveOp
		var index float64
		var row any
		switch {
		case entry.WSLiveOpInsert != nil:
			op.op, index, row = "insert", entry.WSLiveOpInsert.Index, entry.WSLiveOpInsert.Row
		case entry.WSLiveOpUpdate != nil:
			op.op, index, row = "update", entry.WSLiveOpUpdate.Index, entry.WSLiveOpUpdate.Row
		case entry.WSLiveOpDelete != nil:
			op.op, index = "delete", entry.WSLiveOpDelete.Index
		default:
			return nil, invalidResponse("The server sent a live operation of no known kind")
		}
		whole, err := wholeNumber(index, "a live operation index")
		if err != nil {
			return nil, err
		}
		op.index = int(whole)
		if op.op != "delete" {
			mapped, isMap := row.(map[string]any)
			if !isMap {
				return nil, invalidResponse("The server sent a live %s whose row is not an object", op.op)
			}
			op.row = mapped
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func (t *wsTransport) liveMessage(frame serverFrame) {
	t.mutex.Lock()
	live := t.lives[frame.ID]
	t.mutex.Unlock()
	if live == nil {
		return
	}
	handlers := live.spec.handlers
	switch {
	case present(frame.Rows):
		rows, err := decodeRows(frame.Rows)
		if err != nil {
			handlers.fail(err)
			return
		}
		handlers.rows(rows)
	case present(frame.Ops):
		ops, err := liveOpsOf(frame.Ops)
		if err != nil {
			handlers.fail(err)
			return
		}
		handlers.ops(ops)
	case frame.Revalidating:
		handlers.revalidating()
	default:
		handlers.fail(invalidResponse("A live message carried no rows, operations, or status"))
	}
}
