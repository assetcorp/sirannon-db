package tests

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

const (
	validToken     = "client-test-token-6f1c"
	forbiddenToken = "client-test-forbidden-93ad"
	wrongToken     = "client-test-wrong-51be"
)

func asRemoteError(t *testing.T, err error) *sirannon.Error {
	t.Helper()
	var failure *sirannon.Error
	if !errors.As(err, &failure) {
		t.Fatalf("got %v, want a *sirannon.Error", err)
	}
	return failure
}

func assertNoCredential(t *testing.T, err error) {
	t.Helper()
	text := err.Error()
	for _, secret := range []string{validToken, forbiddenToken, wrongToken} {
		if strings.Contains(text, secret) {
			t.Fatalf("the error %q carries a credential", text)
		}
	}
}

func TestRefusedWebSocketStopsReconnecting(t *testing.T) {
	server := startServer(t, serverConfig{Token: validToken, ForbiddenToken: forbiddenToken})
	cases := []struct {
		name      string
		token     string
		code      string
		closeCode int
	}{
		{name: "unauthenticated", token: wrongToken, code: sirannon.CodeUnauthorized, closeCode: 4401},
		{name: "forbidden", token: forbiddenToken, code: sirannon.CodeForbidden, closeCode: 4403},
	}
	for _, refusal := range cases {
		t.Run(refusal.name, func(t *testing.T) {
			ctx := testContext(t)
			relay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", server.port))
			client, err := sirannon.New(relay.url(), sirannon.Options{
				Headers:           map[string]string{"Authorization": "Bearer " + refusal.token},
				ReconnectInterval: 20 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			db, _ := client.Database("refused")
			log := &eventLog{}
			_, err = db.On("items").Subscribe(ctx, log.record, sirannon.OnError(log.fail))
			failure := asRemoteError(t, err)
			if failure.Code != refusal.code || failure.Details["closeCode"] != refusal.closeCode {
				t.Fatalf("subscribe failed with %s and details %v, want %s and close code %d", failure.Code, failure.Details, refusal.code, refusal.closeCode)
			}
			assertNoCredential(t, err)
			if _, err := db.Query(ctx, "SELECT 1", nil); sirannon.ErrorCode(err) != refusal.code {
				t.Fatalf("the first query after the refusal failed with %v, want %s", err, refusal.code)
			}
			accepted := relay.accepted.Load()
			time.Sleep(300 * time.Millisecond)
			for range 3 {
				_, err := db.Query(ctx, "SELECT 1", nil)
				if sirannon.ErrorCode(err) != refusal.code {
					t.Fatalf("a later query failed with %v, want %s", err, refusal.code)
				}
				assertNoCredential(t, err)
				_, err = db.On("items").Subscribe(ctx, log.record)
				if sirannon.ErrorCode(err) != refusal.code {
					t.Fatalf("a later subscription failed with %v, want %s", err, refusal.code)
				}
			}
			if relay.accepted.Load() != accepted {
				t.Fatalf("the client opened %d connections after the refusal", relay.accepted.Load()-accepted)
			}
		})
	}
}

func TestSubscriptionStopsReconnectingWhenTheServerRefusesTheReconnection(t *testing.T) {
	open := startServer(t, serverConfig{})
	guarded := startServer(t, serverConfig{Token: validToken, ForbiddenToken: forbiddenToken})
	ctx := testContext(t)
	relay := startRelay(t, fmt.Sprintf("127.0.0.1:%d", open.port))
	client, err := sirannon.New(relay.url(), sirannon.Options{
		Headers:           map[string]string{"Authorization": "Bearer " + forbiddenToken},
		ReconnectInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	db, _ := client.Database("moved")
	log := &eventLog{}
	if _, err := db.On("items").Subscribe(ctx, log.record, sirannon.OnError(log.fail)); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	relay.retarget(fmt.Sprintf("127.0.0.1:%d", guarded.port))
	relay.cut()
	waitFor(t, 10*time.Second, "the refusal to reach OnError", func() bool {
		log.mutex.Lock()
		defer log.mutex.Unlock()
		return len(log.errors) > 0
	})
	accepted := relay.accepted.Load()
	time.Sleep(400 * time.Millisecond)
	if relay.accepted.Load() != accepted {
		t.Fatalf("the client reconnected %d times after the server refused it", relay.accepted.Load()-accepted)
	}
	log.mutex.Lock()
	failure := asRemoteError(t, log.errors[0])
	log.mutex.Unlock()
	if failure.Code != sirannon.CodeForbidden || failure.Details["closeCode"] != 4403 {
		t.Fatalf("OnError received %s with details %v, want FORBIDDEN and close code 4403", failure.Code, failure.Details)
	}
	assertNoCredential(t, failure)
}

func TestRefusedHTTPRequestsCarryTheServerCodeAndNoCredential(t *testing.T) {
	server := startServer(t, serverConfig{Token: validToken, ForbiddenToken: forbiddenToken})
	cases := []struct {
		token  string
		code   string
		status int
	}{
		{token: wrongToken, code: "TOKEN_REQUIRED", status: 401},
		{token: forbiddenToken, code: "FORBIDDEN_CALLER", status: 403},
	}
	for _, refusal := range cases {
		ctx := testContext(t)
		client, err := sirannon.New(server.url, sirannon.Options{Transport: sirannon.TransportHTTP, Headers: map[string]string{"Authorization": "Bearer " + refusal.token}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Execute(ctx, "refused", sirannon.ExecuteRequest{SQL: insertItemSQL})
		failure := asRemoteError(t, err)
		if failure.Code != refusal.code || failure.Status != refusal.status || failure.Message == "" {
			t.Fatalf("got %s with status %d, want %s with status %d", failure.Code, failure.Status, refusal.code, refusal.status)
		}
		assertNoCredential(t, err)
		client.Close()
	}
	if count := oracleCount(t, server.databaseFile("refused"), "SELECT COUNT(*) FROM sqlite_master WHERE name = 'items'"); count != 0 {
		rows := oracleCount(t, server.databaseFile("refused"), "SELECT COUNT(*) FROM items")
		if rows != 0 {
			t.Fatalf("a refused caller wrote %d rows", rows)
		}
	}
}

func TestStatementsAreRefusedLocallyWhenTheServerTakesNoSQL(t *testing.T) {
	accepts := false
	server := startServer(t, serverConfig{AcceptSQL: &accepts})
	proxy := startCountingProxy(t, server.url)
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			client, err := sirannon.New(proxy.server.URL, sirannon.Options{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			id := "no-sql-" + string(transport)
			db, _ := client.Database(id)
			before := len(proxy.seen())
			attempts := []error{}
			_, err = db.Query(ctx, "SELECT 1", nil)
			attempts = append(attempts, err)
			_, err = db.Execute(ctx, insertItemSQL, []any{1, "x", 1, nil})
			attempts = append(attempts, err)
			_, err = db.Transaction(ctx, []sirannon.Statement{{SQL: insertItemSQL, Params: []any{1, "x", 1, nil}}})
			attempts = append(attempts, err)
			_, err = db.Batch(ctx, insertItemSQL, []any{[]any{1, "x", 1, nil}})
			attempts = append(attempts, err)
			_, err = db.Load(ctx, insertItemSQL, []any{[]any{1, "x", 1, nil}}, sirannon.LoadOptions{})
			attempts = append(attempts, err)
			_, err = client.Query(ctx, id, sirannon.QueryRequest{SQL: "SELECT 1"})
			attempts = append(attempts, err)
			_, err = client.Execute(ctx, id, sirannon.ExecuteRequest{SQL: insertItemSQL})
			attempts = append(attempts, err)
			_, err = client.Transaction(ctx, id, sirannon.TransactionRequest{Statements: []sirannon.TransactionStatement{{SQL: insertItemSQL}}})
			attempts = append(attempts, err)
			_, err = client.Batch(ctx, id, sirannon.BatchRequest{SQL: insertItemSQL})
			attempts = append(attempts, err)
			_, err = client.Load(ctx, id, sirannon.LoadRequest{SQL: insertItemSQL})
			attempts = append(attempts, err)
			for position, err := range attempts {
				if sirannon.ErrorCode(err) != sirannon.CodeSQLNotAccepted {
					t.Fatalf("attempt %d failed with %v, want %s", position, err, sirannon.CodeSQLNotAccepted)
				}
			}
			seen := proxy.seen()[before:]
			if len(seen) != 1 || seen[0] != "GET /capabilities" {
				t.Fatalf("the statements sent %v, want a single GET /capabilities", seen)
			}
			if _, err := sirannon.Execute(ctx, db, putItem, putItemArgs{ID: int64(7), Label: "kept", Amount: int64(1) << 60, Payload: []byte{9}}); err != nil {
				t.Fatalf("a registered write failed on a server that takes no SQL: %v", err)
			}
			rows, err := sirannon.Query(ctx, db, itemByID, itemArgs{ID: int64(7)})
			if err != nil || len(rows) != 1 || rows[0].Amount != int64(1)<<60 {
				t.Fatalf("a registered read gave %v, %v", rows, err)
			}
			if slices.Contains(proxy.seen(), "POST /db/"+id+"/query") {
				t.Fatalf("a statement reached the server")
			}
			assertOracleHolds(t, server.databaseFile(id), []expectedItem{{id: 7, label: "kept", amount: 1 << 60, payload: []byte{9}}})
		})
	}
}
