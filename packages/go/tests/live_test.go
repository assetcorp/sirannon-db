package tests

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

func liveMatchesOracle(t *testing.T, state sirannon.LiveState[itemRow], items []oracleItem) bool {
	t.Helper()
	if state.Status != sirannon.LiveReady || state.Revalidating || len(state.Rows) != len(items) {
		return false
	}
	for position, item := range items {
		row := state.Rows[position]
		payload, _ := row.Payload.([]byte)
		if row.ID != item.id || row.Label != item.label.String || row.Amount != item.amount.Int64 || !bytes.Equal(payload, item.payload) {
			return false
		}
	}
	return true
}

func TestLiveQueryFollowsTheTableAcrossACutConnection(t *testing.T) {
	server := startServer(t, serverConfig{})
	relay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", server.port))
	ctx := testContext(t)
	client, err := sirannon.New(relay.url(), sirannon.Options{ReconnectInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	writer, err := sirannon.New(server.url, sirannon.Options{Transport: sirannon.TransportHTTP})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	const id = "live"
	db, _ := client.Database(id)
	writerDB, _ := writer.Database(id)
	insertItems(t, ctx, writerDB, 1, 3, nil)
	query, err := sirannon.Live(ctx, db, itemsInOrder, sirannon.NoArgs{})
	if err != nil {
		t.Fatalf("open the live query: %v", err)
	}
	var mutex sync.Mutex
	kinds := map[sirannon.LiveUpdateKind]int{}
	stop := query.Subscribe(func(update sirannon.LiveUpdate[itemRow]) {
		mutex.Lock()
		kinds[update.Kind]++
		mutex.Unlock()
	})
	defer stop()
	path := server.databaseFile(id)
	if !liveMatchesOracle(t, query.State(), oracleItems(t, path)) {
		t.Fatalf("the first state %+v does not match the file", query.State())
	}
	insertItems(t, ctx, writerDB, 4, 4, nil)
	waitFor(t, 10*time.Second, "an edit to the live rows", func() bool {
		mutex.Lock()
		defer mutex.Unlock()
		return kinds[sirannon.LiveUpdateOps] > 0
	})
	if !liveMatchesOracle(t, query.State(), oracleItems(t, path)) {
		t.Fatalf("the rows after the first edit %+v do not match the file", query.State())
	}
	insertItems(t, ctx, writerDB, 5, 12, nil)
	if _, err := writerDB.Execute(ctx, "UPDATE items SET amount = ?, payload = ? WHERE id = ?", []any{int64(-1) << 60, []byte{0xde, 0xad}, int64(2)}); err != nil {
		t.Fatal(err)
	}
	if _, err := writerDB.Execute(ctx, "DELETE FROM items WHERE id IN (1, 7)", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "the live rows to match the file", func() bool { return liveMatchesOracle(t, query.State(), oracleItems(t, path)) })
	relay.cut()
	insertItems(t, ctx, writerDB, 13, 20, nil)
	if _, err := writerDB.Execute(ctx, "DELETE FROM items WHERE id = 3", nil); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 15*time.Second, "the live rows to match the file after the cut", func() bool { return liveMatchesOracle(t, query.State(), oracleItems(t, path)) })
	mutex.Lock()
	seenOps, seenRevalidating := kinds[sirannon.LiveUpdateOps], kinds[sirannon.LiveUpdateRevalidating]
	mutex.Unlock()
	if seenOps == 0 || seenRevalidating == 0 {
		t.Fatalf("the listener saw %d op updates and %d revalidating updates, want both", seenOps, seenRevalidating)
	}
	if err := query.Close(ctx); err != nil {
		t.Fatalf("close the live query: %v", err)
	}
	frozen := query.State()
	insertItems(t, ctx, writerDB, 21, 22, nil)
	time.Sleep(300 * time.Millisecond)
	if len(query.State().Rows) != len(frozen.Rows) {
		t.Fatalf("a closed live query kept changing")
	}
}

func TestLiveQueryRefusesHTTPAndUnknownReads(t *testing.T) {
	server := startServer(t, serverConfig{})
	ctx := testContext(t)
	httpClient, err := sirannon.New(server.url, sirannon.Options{Transport: sirannon.TransportHTTP})
	if err != nil {
		t.Fatal(err)
	}
	defer httpClient.Close()
	httpDB, _ := httpClient.Database("live-refusals")
	if _, err := sirannon.Live(ctx, httpDB, itemsInOrder, sirannon.NoArgs{}); sirannon.ErrorCode(err) != sirannon.CodeTransportError {
		t.Fatalf("a live query over HTTP failed with %v, want TRANSPORT_ERROR", err)
	}
	if _, err := httpDB.On("items").Subscribe(ctx, func(sirannon.ChangeEvent) {}); sirannon.ErrorCode(err) != sirannon.CodeTransportError {
		t.Fatalf("a subscription over HTTP failed with %v, want TRANSPORT_ERROR", err)
	}
	wsClient, err := sirannon.New(server.url, sirannon.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer wsClient.Close()
	wsDB, _ := wsClient.Database("live-refusals")
	unknown := sirannon.ReadRef[sirannon.NoArgs, sirannon.Row]{Name: "noSuchRead"}
	_, err = sirannon.Live(ctx, wsDB, unknown, sirannon.NoArgs{})
	failure := asRemoteError(t, err)
	if failure.Code == "" || failure.Code == sirannon.CodeInvalidResponse || failure.Code == sirannon.CodeTimeout {
		t.Fatalf("a live query on an unknown read failed with %v, want the server's refusal", err)
	}
}
