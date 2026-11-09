package tests

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

type eventLog struct {
	mutex  sync.Mutex
	events []sirannon.ChangeEvent
	errors []error
}

func (l *eventLog) record(event sirannon.ChangeEvent) {
	l.mutex.Lock()
	l.events = append(l.events, event)
	l.mutex.Unlock()
}

func (l *eventLog) fail(err error) {
	l.mutex.Lock()
	l.errors = append(l.errors, err)
	l.mutex.Unlock()
}

func (l *eventLog) count() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return len(l.events)
}

func (l *eventLog) failures() []error {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]error{}, l.errors...)
}

func (l *eventLog) snapshot() []sirannon.ChangeEvent {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return append([]sirannon.ChangeEvent{}, l.events...)
}

func insertItems(t *testing.T, ctx context.Context, db *sirannon.RemoteDatabase, from, to int64, between func(id int64)) {
	t.Helper()
	for id := from; id <= to; id++ {
		amount := math.MaxInt64 - id*1_000_000_007
		if _, err := db.Execute(ctx, insertItemSQL, []any{id, fmt.Sprintf("item-%d", id), amount, []byte{0, byte(id), 0xff}}); err != nil {
			t.Fatalf("insert item %d: %v", id, err)
		}
		if between != nil {
			between(id)
		}
	}
}

func oracleChangeSeqs(t *testing.T, path string) []int64 {
	t.Helper()
	handle, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open the oracle on %s: %v", path, err)
	}
	defer handle.Close()
	rows, err := handle.Query("SELECT seq FROM _sirannon_changes WHERE table_name = 'items' ORDER BY seq")
	if err != nil {
		t.Fatalf("read the change log through the oracle: %v", err)
	}
	defer rows.Close()
	var seqs []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatalf("scan a change log seq: %v", err)
		}
		seqs = append(seqs, seq)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate the change log: %v", err)
	}
	return seqs
}

func assertEventsMatchOracle(t *testing.T, events []sirannon.ChangeEvent, path string) {
	t.Helper()
	items := oracleItems(t, path)
	seqs := oracleChangeSeqs(t, path)
	if len(events) != len(items) || len(events) != len(seqs) {
		t.Fatalf("received %d events for %d stored items and %d logged changes", len(events), len(items), len(seqs))
	}
	for position, event := range events {
		item := items[position]
		payload, _ := event.Row["payload"].([]byte)
		matches := event.Type == "insert" && event.Table == "items" && event.Seq == seqs[position] &&
			event.Row["id"] == item.id && event.Row["label"] == item.label.String &&
			item.amountType == "integer" && event.Row["amount"] == item.amount.Int64 &&
			item.payloadType == "blob" && bytes.Equal(payload, item.payload)
		if !matches {
			t.Fatalf("event %d is %s on %s at seq %d with row %v, want seq %d and the stored row %+v", position, event.Type, event.Table, event.Seq, event.Row, seqs[position], item)
		}
	}
}

func TestSubscriptionResumesAcrossACutConnectionWithNoLossOrRepeat(t *testing.T) {
	server := startServer(t, serverConfig{})
	relay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", server.port))
	ctx := testContext(t)
	subscriber, err := sirannon.New(relay.url(), sirannon.Options{ReconnectInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Close()
	writer, err := sirannon.New(server.url, sirannon.Options{Transport: sirannon.TransportHTTP})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	const id = "resume"
	subscribedDB, _ := subscriber.Database(id)
	writerDB, _ := writer.Database(id)
	log := &eventLog{}
	subscription, err := subscribedDB.On("items").Subscribe(ctx, log.record, sirannon.OnError(log.fail))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	const total = 300
	insertItems(t, ctx, writerDB, 1, total, func(item int64) {
		if item%100 == 0 {
			waitFor(t, 10*time.Second, "events before the cut", func() bool { return log.count() > int(item)/2 })
			relay.cut()
		}
	})
	waitFor(t, 30*time.Second, "every change after the cuts", func() bool { return log.count() >= total })
	time.Sleep(300 * time.Millisecond)
	if errors := log.failures(); len(errors) > 0 {
		t.Fatalf("the subscription reported %v", errors)
	}
	assertEventsMatchOracle(t, log.snapshot(), server.databaseFile(id))
	if relay.accepted.Load() < 3 {
		t.Fatalf("the relay accepted %d connections, so the cuts forced no reconnection", relay.accepted.Load())
	}
	if err := subscription.Unsubscribe(ctx); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	insertItems(t, ctx, writerDB, total+1, total+5, nil)
	time.Sleep(300 * time.Millisecond)
	if log.count() != total {
		t.Fatalf("received %d events after unsubscribing, want %d", log.count(), total)
	}
}

func TestSubscriptionResetsWhenTheChangeLogNoLongerHoldsItsCursor(t *testing.T) {
	server := startServer(t, serverConfig{CDCRetentionMs: 100})
	relay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", server.port))
	ctx := testContext(t)
	subscriber, err := sirannon.New(relay.url(), sirannon.Options{ReconnectInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer subscriber.Close()
	keeper, err := sirannon.New(server.url, sirannon.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer keeper.Close()
	const id = "reset"
	subscribedDB, _ := subscriber.Database(id)
	keeperDB, _ := keeper.Database(id)
	keeperLog := &eventLog{}
	if _, err := keeperDB.On("items").Subscribe(ctx, keeperLog.record); err != nil {
		t.Fatalf("keeper subscribe: %v", err)
	}
	log := &eventLog{}
	var resets atomic.Int64
	_, err = subscribedDB.On("items").Subscribe(ctx, log.record, sirannon.OnError(log.fail), sirannon.OnReset(func() { resets.Add(1) }))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	insertItems(t, ctx, keeperDB, 1, 5, nil)
	waitFor(t, 10*time.Second, "the first five changes", func() bool { return log.count() == 5 })
	relay.refuse(true)
	relay.cut()
	insertItems(t, ctx, keeperDB, 6, 25, nil)
	waitFor(t, 10*time.Second, "the keeper to see every change", func() bool { return keeperLog.count() == 25 })
	time.Sleep(6 * time.Second)
	insertItems(t, ctx, keeperDB, 26, 26, nil)
	waitFor(t, 10*time.Second, "the keeper to see the last change", func() bool { return keeperLog.count() == 26 })
	relay.refuse(false)
	waitFor(t, 20*time.Second, "OnReset after the change log dropped the cursor", func() bool { return resets.Load() >= 1 })
	before := log.count()
	insertItems(t, ctx, keeperDB, 27, 27, nil)
	waitFor(t, 10*time.Second, "a change after the reset", func() bool { return log.count() > before })
	events := log.snapshot()
	last := events[len(events)-1]
	if last.Row["id"] != int64(27) {
		t.Fatalf("the change after the reset carries row %v, want item 27", last.Row)
	}
	for _, event := range events[5:] {
		if id, _ := event.Row["id"].(int64); id >= 6 && id <= 25 {
			t.Fatalf("the subscription replayed item %d, which the change log no longer held", id)
		}
	}
}
