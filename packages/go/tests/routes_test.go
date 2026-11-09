package tests

import (
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

var clientOriginatedCodes = []string{
	sirannon.CodeConnectionError, sirannon.CodeTimeout, sirannon.CodeTransportError,
	sirannon.CodeInvalidResponse, sirannon.CodeRoutingError, sirannon.CodeInvalidArgument, sirannon.CodeSQLNotAccepted,
}

type routeLedger struct {
	t      *testing.T
	called []string
}

func (l *routeLedger) answered(name string, err error) {
	l.t.Helper()
	l.called = append(l.called, name)
	if err == nil {
		return
	}
	failure := asRemoteError(l.t, err)
	if failure.Status < 400 || failure.Code == "" || slices.Contains(clientOriginatedCodes, failure.Code) {
		l.t.Fatalf("%s gave %v with status %d, want a server answer", name, err, failure.Status)
	}
	assertNoCredential(l.t, err)
}

func (l *routeLedger) succeeded(name string, err error) {
	l.t.Helper()
	l.called = append(l.called, name)
	if err != nil {
		l.t.Fatalf("%s: %v", name, err)
	}
}

func TestEveryGeneratedRouteReachesTheServer(t *testing.T) {
	server := startServer(t, serverConfig{Token: validToken})
	ctx := testContext(t)
	client, err := sirannon.New(server.url, sirannon.Options{Transport: sirannon.TransportHTTP, Headers: map[string]string{"Authorization": "Bearer " + validToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ledger := &routeLedger{t: t}
	const id = "routes"
	big, blob := int64(-1)<<62-3, []byte{0, 1, 0xfe, 0xff}
	capabilities, err := client.GetCapabilities(ctx)
	ledger.succeeded("GetCapabilities", err)
	if !slices.Contains(capabilities.Capabilities, "query.sql") || capabilities.Registry == nil || capabilities.Registry.Digest == "" {
		t.Fatalf("capabilities %+v lack query.sql or the registry digest", capabilities)
	}
	_, err = client.GetHealth(ctx)
	ledger.succeeded("GetHealth", err)
	_, err = client.GetReadiness(ctx)
	ledger.answered("GetReadiness", err)
	_, err = client.Execute(ctx, id, sirannon.ExecuteRequest{SQL: insertItemSQL, Params: &sirannon.ExecuteRequestParams{List: []any{int64(1), "execute", big, blob}}})
	ledger.succeeded("Execute", err)
	_, err = client.Transaction(ctx, id, sirannon.TransactionRequest{Statements: []sirannon.TransactionStatement{{SQL: insertItemSQL, Params: &sirannon.TransactionStatementParams{List: []any{int64(2), "transaction", big + 1, blob}}}}})
	ledger.succeeded("Transaction", err)
	_, err = client.Batch(ctx, id, sirannon.BatchRequest{SQL: insertItemSQL, ParamsBatch: []sirannon.BatchRequestParamsBatchItem{{List: []any{int64(3), "batch", big + 2, blob}}}})
	ledger.succeeded("Batch", err)
	_, err = client.Load(ctx, id, sirannon.LoadRequest{SQL: insertItemSQL, ParamsBatch: []sirannon.LoadRequestParamsBatchItem{{List: []any{int64(4), "load", big + 3, blob}}}})
	ledger.succeeded("Load", err)
	_, err = client.ExecuteOperation(ctx, id, "putItem", &sirannon.OperationExecuteRequest{Args: map[string]any{"id": int64(5), "label": "operation", "amount": big + 4, "payload": blob}})
	ledger.succeeded("ExecuteOperation", err)
	queried, err := client.Query(ctx, id, sirannon.QueryRequest{SQL: "SELECT id, label, amount, payload FROM items ORDER BY id"})
	ledger.succeeded("Query", err)
	named, err := client.QueryOperation(ctx, id, "itemById", &sirannon.OperationQueryRequest{Args: map[string]any{"id": int64(5)}})
	ledger.succeeded("QueryOperation", err)
	expected := []expectedItem{
		{1, "execute", big, blob}, {2, "transaction", big + 1, blob}, {3, "batch", big + 2, blob},
		{4, "load", big + 3, blob}, {5, "operation", big + 4, blob},
	}
	assertOracleHolds(t, server.databaseFile(id), expected)
	for position, want := range expected {
		assertRowMatches(t, queried.Rows[position], want)
	}
	if len(named.Rows) != 1 {
		t.Fatalf("itemById gave %d rows", len(named.Rows))
	}
	assertRowMatches(t, named.Rows[0], expected[4])
	_, err = client.GetClusterStatus(ctx, id)
	ledger.answered("GetClusterStatus", err)
	_, err = client.ListMigrations(ctx, id, sirannon.MigrationListRequest{})
	ledger.answered("ListMigrations", err)
	_, err = client.GetSnapshotManifest(ctx, id)
	ledger.answered("GetSnapshotManifest", err)
	page, err := client.GetSnapshotPage(ctx, id, sirannon.SnapshotPageRequest{Table: "items"})
	ledger.answered("GetSnapshotPage", err)
	if err == nil && len(page.Rows) != len(expected) {
		t.Fatalf("the snapshot page holds %d rows, want %d", len(page.Rows), len(expected))
	}
	_, err = client.ApplyChanges(ctx, id, sirannon.ChangesRequest{Batch: sirannon.SyncBatch{SourceNodeID: "device", BatchID: "b1", FromSeq: "1", ToSeq: "1", Checksum: "bad"}})
	ledger.answered("ApplyChanges", err)
	_, err = client.GetBackupStatus(ctx, id)
	ledger.answered("GetBackupStatus", err)
	_, err = client.TriggerBackup(ctx, id)
	ledger.answered("TriggerBackup", err)
	_, err = client.ListBackupChains(ctx, id)
	ledger.answered("ListBackupChains", err)
	_, err = client.VerifyBackup(ctx, id, sirannon.BackupVerifyRequest{Name: "no-such-backup"})
	ledger.answered("VerifyBackup", err)
	_, err = client.ListBackupsSafeToDelete(ctx, id, nil)
	ledger.answered("ListBackupsSafeToDelete", err)
	_, err = client.GetBackupRestoreStatus(ctx, id)
	ledger.answered("GetBackupRestoreStatus", err)
	_, err = client.GetEncryptionStatus(ctx, id)
	ledger.answered("GetEncryptionStatus", err)
	_, err = client.ChangeEncryption(ctx, id, sirannon.EncryptionChangeRequest{KeyRef: sirannon.Ptr("missing-key")})
	ledger.answered("ChangeEncryption", err)
	_, err = client.RestoreBackup(ctx, id, &sirannon.BackupRestoreRequest{Moment: sirannon.Ptr(float64(time.Now().UnixMilli()))})
	ledger.answered("RestoreBackup", err)
	operations := reflect.TypeFor[*sirannon.Operations]()
	for position := range operations.NumMethod() {
		name := operations.Method(position).Name
		if !slices.Contains(ledger.called, name) {
			t.Fatalf("the generated route %s went untested", name)
		}
	}
}

func TestEveryWebSocketMessageRoundTrips(t *testing.T) {
	server := startServer(t, serverConfig{Token: validToken})
	ctx := testContext(t)
	client, err := sirannon.New(server.url, sirannon.Options{WebSocketProtocols: []string{validToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	const id = "frames"
	db, _ := client.Database(id)
	log := &eventLog{}
	subscription, err := db.On("items").Filter(map[string]any{"label": "watched"}).Subscribe(ctx, log.record, sirannon.OnError(log.fail))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	live, err := sirannon.Live(ctx, db, itemsInOrder, sirannon.NoArgs{})
	if err != nil {
		t.Fatalf("live: %v", err)
	}
	if _, err := db.Execute(ctx, insertItemSQL, []any{int64(1), "watched", int64(1) << 55, []byte{1}}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	var statements []sirannon.Statement
	for item := int64(2); item <= 40; item++ {
		statements = append(statements, sirannon.Statement{SQL: insertItemSQL, Params: []any{item, "watched", item << 50, []byte{byte(item)}}})
	}
	if _, err := db.Transaction(ctx, statements); err != nil {
		t.Fatalf("transaction: %v", err)
	}
	if _, err := db.Batch(ctx, insertItemSQL, []any{[]any{int64(41), "ignored", int64(0), nil}}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := db.Load(ctx, insertItemSQL, []any{[]any{int64(42), "ignored", int64(0), nil}}, sirannon.LoadOptions{Durability: sirannon.BulkLoadDurabilityNormal}); err != nil {
		t.Fatalf("load: %v", err)
	}
	waitFor(t, 10*time.Second, "the filtered changes", func() bool { return log.count() == 40 })
	assertInsertsWithAmounts(t, log.snapshot())
	waitFor(t, 10*time.Second, "the live rows", func() bool {
		return liveMatchesOracle(t, live.State(), oracleItems(t, server.databaseFile(id)))
	})
	_, err = db.Query(ctx, "SELECT * FROM no_such_table", nil)
	failure := asRemoteError(t, err)
	if slices.Contains(clientOriginatedCodes, failure.Code) || failure.Message == "" {
		t.Fatalf("a failing query gave %v, want the server's error frame", err)
	}
	_, err = db.Ack(ctx, "device-frames", 1)
	if err != nil {
		failure := asRemoteError(t, err)
		if slices.Contains(clientOriginatedCodes, failure.Code) {
			t.Fatalf("ack gave %v, want a server answer", err)
		}
	}
	if err := subscription.Unsubscribe(ctx); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}
	if err := live.Close(ctx); err != nil {
		t.Fatalf("close the live query: %v", err)
	}
	if _, err := db.Execute(ctx, insertItemSQL, []any{int64(43), "watched", int64(0), nil}); err != nil {
		t.Fatalf("execute after unsubscribing: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if log.count() != 40 {
		t.Fatalf("the subscription delivered %d changes after it ended", log.count()-40)
	}
}

func assertInsertsWithAmounts(t *testing.T, events []sirannon.ChangeEvent) {
	t.Helper()
	for position, event := range events {
		item := int64(position + 1)
		want := item << 50
		if item == 1 {
			want = int64(1) << 55
		}
		if event.Row["id"] != item || event.Row["amount"] != want || event.Row["label"] != "watched" {
			t.Fatalf("change %d carries row %v, want item %d with amount %d", position, event.Row, item, want)
		}
	}
}

func TestClosedClientsLeaveNoGoroutinesBehind(t *testing.T) {
	server := startServer(t, serverConfig{})
	ctx := testContext(t)
	baseline := runtime.NumGoroutine()
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		client, err := sirannon.New(server.url, sirannon.Options{Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"leak-a", "leak-b"} {
			db, _ := client.Database(id + "-" + string(transport))
			if _, err := db.Execute(ctx, insertItemSQL, []any{int64(1), "x", int64(1), nil}); err != nil {
				t.Fatalf("execute: %v", err)
			}
			if transport == sirannon.TransportWebSocket {
				if _, err := db.On("items").Subscribe(ctx, func(sirannon.ChangeEvent) {}); err != nil {
					t.Fatalf("subscribe: %v", err)
				}
				if _, err := sirannon.Live(ctx, db, itemsInOrder, sirannon.NoArgs{}); err != nil {
					t.Fatalf("live: %v", err)
				}
			}
		}
		client.Close()
	}
	topology, err := sirannon.NewTopology(sirannon.TopologyOptions{Primary: server.url, Replicas: []string{server.url}, ReadPreference: sirannon.ReadFromNearest})
	if err != nil {
		t.Fatal(err)
	}
	db, _ := topology.Database("leak-c")
	if _, err := db.Query(ctx, "SELECT 1", nil); err != nil {
		t.Fatalf("topology query: %v", err)
	}
	if _, err := db.On("items").Subscribe(ctx, func(sirannon.ChangeEvent) {}); err != nil {
		t.Fatalf("topology subscribe: %v", err)
	}
	topology.Close()
	waitFor(t, 5*time.Second, "goroutines to return to the baseline", func() bool { return runtime.NumGoroutine() <= baseline })
}
