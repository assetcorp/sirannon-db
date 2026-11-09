package tests

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
	"github.com/coder/websocket"
)

const malformedRowsBody = `{"rows":[{"n":{"__sirannon_int":"12x"}}]}`

func TestLiveQueryFailsOnAnOperationOutsideItsRows(t *testing.T) {
	var liveID any
	fake := startFakeServer(t, func(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
		switch frame["type"] {
		case "subscribe":
			liveID = frame["id"]
			sendFrame(ctx, conn, map[string]any{"type": "subscribed", "id": liveID, "rows": []any{map[string]any{"id": 1}}})
			sendFrame(ctx, conn, map[string]any{"type": "live", "id": liveID, "ops": []any{
				map[string]any{"op": "insert", "index": 1, "row": map[string]any{"id": 2}},
				map[string]any{"op": "update", "index": 0, "row": map[string]any{"id": map[string]any{"__sirannon_int": "9007199254740993"}}},
				map[string]any{"op": "delete", "index": 1},
			}})
		case "query":
			sendFrame(ctx, conn, map[string]any{"type": "result", "id": frame["id"], "data": map[string]any{"rows": []any{}}})
			sendFrame(ctx, conn, map[string]any{"type": "live", "id": liveID, "ops": []any{
				map[string]any{"op": "update", "index": 3, "row": map[string]any{"id": 9}},
			}})
		}
	}, nil)
	ctx := testContext(t)
	client, err := sirannon.New(fake.server.URL, sirannon.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	db, _ := client.Database("hostile")
	query, err := sirannon.Live(ctx, db, sirannon.ReadRef[sirannon.NoArgs, sirannon.Row]{Name: "everything"}, sirannon.NoArgs{})
	if err != nil {
		t.Fatalf("open the live query: %v", err)
	}
	waitFor(t, 5*time.Second, "the valid operations to apply", func() bool {
		rows := query.State().Rows
		return len(rows) == 1 && rows[0]["id"] == int64(9007199254740993)
	})
	if _, err := db.Query(ctx, "SELECT 1", nil); err != nil {
		t.Fatalf("trigger the bad operation: %v", err)
	}
	waitFor(t, 5*time.Second, "the live query to fail", func() bool { return query.State().Status == sirannon.LiveFailed })
	if code := sirannon.ErrorCode(query.State().Err); code != sirannon.CodeInvalidResponse {
		t.Fatalf("the live query failed with %v, want INVALID_RESPONSE", query.State().Err)
	}
}

func TestMalformedEnvelopesFailWithInvalidResponse(t *testing.T) {
	bodies := []string{
		malformedRowsBody,
		`{"rows":[{"n":{"__sirannon_int":"99999999999999999999"}}]}`,
		`{"rows":[{"b":{"__sirannon_blob":"ABC"}}]}`,
		`{"rows":[{"b":{"__sirannon_blob":"ZZ"}}]}`,
	}
	for _, body := range bodies {
		fake := startFakeServer(t, func(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
			sendRaw(ctx, conn, `{"type":"result","id":"`+frame["id"].(string)+`","data":`+body+`}`)
		}, func(writer http.ResponseWriter, _ *http.Request) {
			writeJSON(writer, body)
		})
		ctx := testContext(t)
		for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
			client, err := sirannon.New(fake.server.URL, sirannon.Options{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			db, _ := client.Database("hostile")
			if _, err := db.Query(ctx, "SELECT 1", nil); sirannon.ErrorCode(err) != sirannon.CodeInvalidResponse {
				t.Fatalf("%s over %s gave %v, want INVALID_RESPONSE", body, transport, err)
			}
			if _, err := client.Query(ctx, "hostile", sirannon.QueryRequest{SQL: "SELECT 1"}); sirannon.ErrorCode(err) != sirannon.CodeInvalidResponse {
				t.Fatalf("%s through the generated route gave %v, want INVALID_RESPONSE", body, err)
			}
			client.Close()
		}
	}
}

func TestCloseAndDeadlinesEndRequestsTheServerNeverAnswers(t *testing.T) {
	fake := startFakeServer(t, nil, nil)
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			client, err := sirannon.New(fake.server.URL, sirannon.Options{Transport: transport, RequestTimeout: sirannon.NoTimeout})
			if err != nil {
				t.Fatal(err)
			}
			db, _ := client.Database("silent")
			for len(fake.received) > 0 {
				<-fake.received
			}
			outcome := make(chan error, 1)
			go func() {
				_, err := db.Query(context.Background(), "SELECT 1", nil)
				outcome <- err
			}()
			<-fake.received
			started := time.Now()
			client.Close()
			select {
			case err := <-outcome:
				if sirannon.ErrorCode(err) != sirannon.CodeTransportError {
					t.Fatalf("a pending request ended with %v after Close, want TRANSPORT_ERROR", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("a pending request outlived Close")
			}
			if elapsed := time.Since(started); elapsed > 3*time.Second {
				t.Fatalf("Close took %s", elapsed)
			}
			if _, err := db.Query(context.Background(), "SELECT 1", nil); sirannon.ErrorCode(err) != sirannon.CodeTransportError {
				t.Fatalf("a request after Close failed with %v, want TRANSPORT_ERROR", err)
			}
			timed, err := sirannon.New(fake.server.URL, sirannon.Options{Transport: transport, RequestTimeout: 150 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer timed.Close()
			timedDB, _ := timed.Database("silent")
			if _, err := timedDB.Query(context.Background(), "SELECT 1", nil); sirannon.ErrorCode(err) != sirannon.CodeTimeout {
				t.Fatalf("a request past RequestTimeout failed with %v, want TIMEOUT", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			started = time.Now()
			if _, err := timedDB.Execute(ctx, insertItemSQL, []any{1, "x", 1, nil}); sirannon.ErrorCode(err) != sirannon.CodeTimeout || time.Since(started) > time.Second {
				t.Fatalf("a request past its context deadline failed with %v after %s", err, time.Since(started))
			}
			pending := make(chan error, 1)
			go func() {
				_, err := timedDB.Transaction(context.Background(), []sirannon.Statement{{SQL: insertItemSQL}}, sirannon.WithWriteConcern(sirannon.WriteConcernLevelMajority, time.Second))
				pending <- err
			}()
			time.Sleep(50 * time.Millisecond)
			timedDB.Close()
			if err := <-pending; sirannon.ErrorCode(err) != sirannon.CodeTransportError {
				t.Fatalf("a pending request ended with %v after the database closed, want TRANSPORT_ERROR", err)
			}
		})
	}
}

func TestOversizedAnswersAreRefusedAtTheLimit(t *testing.T) {
	huge := `{"rows":[{"text":"` + strings.Repeat("x", 3<<20) + `"}]}`
	fake := startFakeServer(t, func(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
		sendRaw(ctx, conn, `{"type":"result","id":"`+frame["id"].(string)+`","data":`+huge+`}`)
	}, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, huge)
	})
	ctx := testContext(t)
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		client, err := sirannon.New(fake.server.URL, sirannon.Options{Transport: transport, MaxResponseBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		db, _ := client.Database("huge")
		_, err = db.Query(ctx, "SELECT 1", nil)
		code := sirannon.ErrorCode(err)
		if code != sirannon.CodeInvalidResponse && code != sirannon.CodeConnectionError {
			t.Fatalf("an answer over the limit through %s gave %v", transport, err)
		}
		client.Close()
	}
}

func TestIdentifiersArePercentEncodedInPaths(t *testing.T) {
	fake := startFakeServer(t, func(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
		sendRaw(ctx, conn, `{"type":"result","id":"`+frame["id"].(string)+`","data":{"rows":[]}}`)
	}, func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, `{"rows":[]}`)
	})
	ctx := testContext(t)
	const awkward = "a/b?c#d ..%"
	const encoded = "a%2Fb%3Fc%23d%20..%25"
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		client, err := sirannon.New(fake.server.URL, sirannon.Options{Transport: transport})
		if err != nil {
			t.Fatal(err)
		}
		db, _ := client.Database(awkward)
		if _, err := db.Query(ctx, "SELECT 1", nil); err != nil {
			t.Fatalf("query through %s: %v", transport, err)
		}
		if _, err := sirannon.Query(ctx, db, sirannon.ReadRef[sirannon.NoArgs, sirannon.Row]{Name: "x/y?z"}, sirannon.NoArgs{}); err != nil {
			t.Fatalf("registered read through %s: %v", transport, err)
		}
		dots, _ := client.Database("..")
		if _, err := dots.Query(ctx, "SELECT 1", nil); err != nil {
			t.Fatalf("query on the dot-dot database through %s: %v", transport, err)
		}
		client.Close()
	}
	httpClient, _ := sirannon.New(fake.server.URL, sirannon.Options{Transport: sirannon.TransportHTTP})
	defer httpClient.Close()
	if _, err := httpClient.QueryOperation(ctx, awkward, "x/y?z", nil); err != nil {
		t.Fatalf("generated registered read: %v", err)
	}
	uris := fake.seenURIs()
	for _, want := range []string{
		"GET /db/" + encoded,
		"POST /db/" + encoded + "/query",
		"POST /db/" + encoded + "/query/x%2Fy%3Fz",
		"POST /db/%2E%2E/query",
		"GET /db/%2E%2E",
	} {
		if !slices.Contains(uris, want) {
			t.Fatalf("no request went to %q; the server saw %v", want, uris)
		}
	}
}
