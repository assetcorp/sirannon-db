package tests

import (
	"bytes"
	"math"
	"testing"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
	"github.com/assetcorp/sirannon-db/packages/go/tests/testoperations"
)

type (
	itemArgs       = testoperations.SharedItemByIDArgs
	removeItemArgs = testoperations.SharedRemoveItemArgs
	putItemArgs    = testoperations.SharedPutItemArgs
	itemRow        = testoperations.SharedItemsInOrderRow
)

var (
	itemByID     = testoperations.SharedItemByID
	itemsInOrder = testoperations.SharedItemsInOrder
	putItem      = testoperations.SharedPutItem
	removeItem   = testoperations.SharedRemoveItem
)

const insertItemSQL = "INSERT INTO items (id, label, amount, payload) VALUES (?, ?, ?, ?)"

type expectedItem struct {
	id      int64
	label   string
	amount  int64
	payload []byte
}

var edgeAmounts = []int64{
	1<<53 + 1,
	-(1<<53 + 1),
	math.MaxInt64,
	math.MinInt64,
	-1,
	0,
	1<<53 - 1,
	-(1 << 62) - 7,
}

func edgePayload(seed int) []byte {
	payload := make([]byte, 64+seed)
	for position := range payload {
		payload[position] = byte((position*31 + seed*7) % 256)
	}
	payload[0] = 0
	return payload
}

func assertOracleHolds(t *testing.T, path string, expected []expectedItem) {
	t.Helper()
	items := oracleItems(t, path)
	if len(items) != len(expected) {
		t.Fatalf("the file holds %d items, want %d", len(items), len(expected))
	}
	for position, want := range expected {
		got := items[position]
		if got.id != want.id || got.label.String != want.label || got.amount.Int64 != want.amount {
			t.Fatalf("item %d in the file is (%d, %q, %d), want (%d, %q, %d)", position, got.id, got.label.String, got.amount.Int64, want.id, want.label, want.amount)
		}
		if got.amountType != "integer" || got.payloadType != "blob" {
			t.Fatalf("item %d stores amount as %s and payload as %s, want integer and blob", want.id, got.amountType, got.payloadType)
		}
		if !bytes.Equal(got.payload, want.payload) {
			t.Fatalf("item %d stores payload %x, want %x", want.id, got.payload, want.payload)
		}
	}
}

func assertRowMatches(t *testing.T, row map[string]any, want expectedItem) {
	t.Helper()
	if row["id"] != want.id || row["label"] != want.label || row["amount"] != want.amount {
		t.Fatalf("row %v does not match item %d (%q, %d)", row, want.id, want.label, want.amount)
	}
	payload, isBytes := row["payload"].([]byte)
	if !isBytes || !bytes.Equal(payload, want.payload) {
		t.Fatalf("row %d carries payload %#v, want bytes %x", want.id, row["payload"], want.payload)
	}
}

func TestStatementsCarryWideIntegersAndBlobsExactly(t *testing.T) {
	server := startServer(t, serverConfig{})
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			client, err := sirannon.New(server.url, sirannon.Options{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			id := "values-" + string(transport)
			db, err := client.Database(id)
			if err != nil {
				t.Fatal(err)
			}
			var expected []expectedItem
			for position, amount := range edgeAmounts[:2] {
				item := expectedItem{id: int64(position + 1), label: "execute", amount: amount, payload: edgePayload(position)}
				if _, err := db.Execute(ctx, insertItemSQL, []any{item.id, item.label, item.amount, item.payload}); err != nil {
					t.Fatalf("execute item %d: %v", item.id, err)
				}
				expected = append(expected, item)
			}
			var batch []any
			for position, amount := range edgeAmounts[2:4] {
				item := expectedItem{id: int64(position + 10), label: "batch", amount: amount, payload: edgePayload(position + 10)}
				batch = append(batch, []any{item.id, item.label, item.amount, item.payload})
				expected = append(expected, item)
			}
			if results, err := db.Batch(ctx, insertItemSQL, batch); err != nil || len(results) != 2 {
				t.Fatalf("batch: %v, %d results", err, len(results))
			}
			var statements []sirannon.Statement
			for position, amount := range edgeAmounts[4:6] {
				item := expectedItem{id: int64(position + 20), label: "transaction", amount: amount, payload: edgePayload(position + 20)}
				statements = append(statements, sirannon.Statement{SQL: insertItemSQL, Params: []any{item.id, item.label, item.amount, item.payload}})
				expected = append(expected, item)
			}
			results, err := db.Transaction(ctx, statements)
			if err != nil || len(results) != 2 || results[1].LastInsertRowID != 21 {
				t.Fatalf("transaction: %v, %+v", err, results)
			}
			var loaded []any
			for position, amount := range edgeAmounts[6:] {
				item := expectedItem{id: int64(position + 30), label: "load", amount: amount, payload: edgePayload(position + 30)}
				loaded = append(loaded, []any{item.id, item.label, item.amount, item.payload})
				expected = append(expected, item)
			}
			if summary, err := db.Load(ctx, insertItemSQL, loaded, sirannon.LoadOptions{}); err != nil || summary.RowsLoaded != 2 {
				t.Fatalf("load: %v, %+v", err, summary)
			}
			assertOracleHolds(t, server.databaseFile(id), expected)
			rows, err := db.Query(ctx, "SELECT id, label, amount, payload FROM items ORDER BY id", nil)
			if err != nil || len(rows) != len(expected) {
				t.Fatalf("query all: %v, %d rows", err, len(rows))
			}
			for position, want := range expected {
				assertRowMatches(t, rows[position], want)
			}
			for _, want := range expected {
				rows, err := db.Query(ctx, "SELECT id, label, amount, payload FROM items WHERE amount = :amount AND payload = :payload", map[string]any{"amount": want.amount, "payload": want.payload})
				if err != nil || len(rows) != 1 {
					t.Fatalf("query by amount %d: %v, %d rows", want.amount, err, len(rows))
				}
				assertRowMatches(t, rows[0], want)
			}
		})
	}
}

func TestRegisteredOperationsCarryWideIntegersAndBlobsExactly(t *testing.T) {
	server := startServer(t, serverConfig{})
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			client, err := sirannon.New(server.url, sirannon.Options{Transport: transport})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			id := "operations-" + string(transport)
			db, err := client.Database(id)
			if err != nil {
				t.Fatal(err)
			}
			var expected []expectedItem
			for position, amount := range edgeAmounts {
				item := expectedItem{id: int64(position + 1), label: "registered", amount: amount, payload: edgePayload(position)}
				results, err := sirannon.Execute(ctx, db, putItem, putItemArgs{ID: item.id, Label: item.label, Amount: item.amount, Payload: item.payload})
				if err != nil || len(results) != 1 || results[0].Changes != 1 {
					t.Fatalf("putItem %d: %v, %+v", item.id, err, results)
				}
				expected = append(expected, item)
			}
			assertOracleHolds(t, server.databaseFile(id), expected)
			for _, want := range expected {
				rows, err := sirannon.Query(ctx, db, itemByID, itemArgs{ID: want.id})
				if err != nil || len(rows) != 1 {
					t.Fatalf("itemById %d: %v, %d rows", want.id, err, len(rows))
				}
				assertRowMatches(t, map[string]any{"id": rows[0].ID, "label": rows[0].Label, "amount": rows[0].Amount, "payload": rows[0].Payload}, want)
			}
			if _, err := sirannon.Execute(ctx, db, removeItem, removeItemArgs{ID: expected[0].id}); err != nil {
				t.Fatalf("removeItem: %v", err)
			}
			expected = expected[1:]
			assertOracleHolds(t, server.databaseFile(id), expected)
			rows, err := sirannon.Query(ctx, db, itemsInOrder, sirannon.NoArgs{})
			if err != nil || len(rows) != len(expected) {
				t.Fatalf("itemsInOrder: %v, %d rows", err, len(rows))
			}
			for position, want := range expected {
				row := rows[position]
				assertRowMatches(t, map[string]any{"id": row.ID, "label": row.Label, "amount": row.Amount, "payload": row.Payload}, want)
			}
		})
	}
}
