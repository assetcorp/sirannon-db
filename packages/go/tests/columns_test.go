package tests

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/websocket"

	sirannon "github.com/assetcorp/sirannon-db/packages/go"
)

func columnsClient(t *testing.T, url string, transport sirannon.TransportKind, id string) *sirannon.RemoteDatabase {
	t.Helper()
	client, err := sirannon.New(url, sirannon.Options{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	db, err := client.Database(id)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func oracleRowBytes(t *testing.T, path string, upToID int64) []int64 {
	t.Helper()
	handle, err := sql.Open("sqlite", "file:"+path+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	rows, err := handle.Query("SELECT 32 + coalesce(length(CAST(label AS BLOB)), 0) + coalesce(length(payload), 0) FROM items WHERE id <= ? ORDER BY id", upToID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var sizes []int64
	for rows.Next() {
		var size int64
		if err := rows.Scan(&size); err != nil {
			t.Fatal(err)
		}
		sizes = append(sizes, size)
	}
	return sizes
}

func truncation(answer sirannon.QueryColumnsResponse) string {
	if answer.Truncated == nil {
		return "none"
	}
	return *answer.Truncated
}

func TestQueryWithColumnsKeepsRepeatedNamesAndExactValuesInColumnOrder(t *testing.T) {
	server := startServer(t, serverConfig{})
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			db := columnsClient(t, server.url, transport, "columns-order-"+string(transport))
			payload := edgePayload(3)
			if _, err := db.Execute(ctx, insertItemSQL, []any{int64(7), "héllo", edgeAmounts[2], payload}); err != nil {
				t.Fatal(err)
			}
			answer, err := db.QueryWithColumns(ctx, `SELECT a.id, b.id, a.amount, a.payload, a.label AS "1", a.amount + 0 AS sum FROM items a JOIN items b ON a.id = b.id`, nil, sirannon.ColumnLimits{})
			if err != nil {
				t.Fatal(err)
			}
			names := make([]string, len(answer.Columns))
			for position, column := range answer.Columns {
				names[position] = column.Name
			}
			if strings.Join(names, ",") != "id,id,amount,payload,1,sum" {
				t.Fatalf("the columns are %v", names)
			}
			first := answer.Columns[0]
			if first.Table == nil || *first.Table != "items" || first.Column == nil || *first.Column != "id" || first.DeclaredType == nil || *first.DeclaredType != "INTEGER" {
				t.Fatalf("the first column reports %+v, want items.id declared INTEGER", first)
			}
			expression := answer.Columns[5]
			if expression.Table != nil || expression.Column != nil || expression.DeclaredType != nil || expression.Database != nil {
				t.Fatalf("the expression column reports %+v, want every detail null", expression)
			}
			if len(answer.Rows) != 1 || answer.Truncated != nil {
				t.Fatalf("the answer holds %d rows, truncated %s", len(answer.Rows), truncation(answer))
			}
			row := answer.Rows[0]
			if row[0] != int64(7) || row[1] != int64(7) || row[2] != edgeAmounts[2] || row[4] != "héllo" || row[5] != edgeAmounts[2] {
				t.Fatalf("the row is %#v", row)
			}
			if got, isBytes := row[3].([]byte); !isBytes || !bytes.Equal(got, payload) {
				t.Fatalf("the payload is %#v, want %x", row[3], payload)
			}
		})
	}
}

func TestQueryWithColumnsStopsAtEveryLimitThatAHostileReadReaches(t *testing.T) {
	server := startServer(t, serverConfig{})
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			id := "columns-limits-" + string(transport)
			db := columnsClient(t, server.url, transport, id)
			for position := range 5 {
				label := strings.Repeat("x", 100*(position+1))
				if _, err := db.Execute(ctx, insertItemSQL, []any{int64(position + 1), label, int64(position), bytes.Repeat([]byte{0xab}, 4096)}); err != nil {
					t.Fatal(err)
				}
			}
			sizes := oracleRowBytes(t, server.databaseFile(id), 5)
			budget := sizes[0] + sizes[1] + sizes[2] - 1
			answer, err := db.QueryWithColumns(ctx, "SELECT id, label, amount, payload FROM items ORDER BY id", nil, sirannon.ColumnLimits{MaxBytes: sirannon.Ptr(budget)})
			if err != nil || len(answer.Rows) != 2 || truncation(answer) != "bytes" {
				t.Fatalf("a budget one byte short of three rows gave %v, %d rows, truncated %s", err, len(answer.Rows), truncation(answer))
			}
			answer, err = db.QueryWithColumns(ctx, "SELECT id, label, amount, payload FROM items ORDER BY id", nil, sirannon.ColumnLimits{MaxBytes: sirannon.Ptr(budget + 1)})
			if err != nil || len(answer.Rows) != 3 || truncation(answer) != "bytes" {
				t.Fatalf("a budget of exactly three rows gave %v, %d rows, truncated %s", err, len(answer.Rows), truncation(answer))
			}
			answer, err = db.QueryWithColumns(ctx, "SELECT id FROM items ORDER BY id", nil, sirannon.ColumnLimits{MaxRows: sirannon.Ptr(int64(5))})
			if err != nil || len(answer.Rows) != 5 || answer.Truncated != nil {
				t.Fatalf("a row limit equal to the row count gave %v, %d rows, truncated %s", err, len(answer.Rows), truncation(answer))
			}
			answer, err = db.QueryWithColumns(ctx, "SELECT id FROM items ORDER BY id", nil, sirannon.ColumnLimits{MaxRows: sirannon.Ptr(int64(0))})
			if err != nil || len(answer.Rows) != 0 || len(answer.Columns) != 1 || truncation(answer) != "rows" {
				t.Fatalf("a row limit of zero gave %v, %d rows, %d columns, truncated %s", err, len(answer.Rows), len(answer.Columns), truncation(answer))
			}
			endless := "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n) SELECT i, zeroblob(65536) FROM n"
			answer, err = db.QueryWithColumns(ctx, endless, nil, sirannon.ColumnLimits{MaxBytes: sirannon.Ptr(int64(1 << 20))})
			if err != nil || truncation(answer) != "bytes" || len(answer.Rows) != 15 {
				t.Fatalf("an endless read of 64 KiB blobs under 1 MiB gave %v, %d rows, truncated %s", err, len(answer.Rows), truncation(answer))
			}
			answer, err = db.QueryWithColumns(ctx, endless, nil, sirannon.ColumnLimits{MaxRows: sirannon.Ptr(int64(3))})
			if err != nil || truncation(answer) != "rows" || len(answer.Rows) != 3 {
				t.Fatalf("an endless read under three rows gave %v, %d rows, truncated %s", err, len(answer.Rows), truncation(answer))
			}
		})
	}
}

func TestQueryWithColumnsRefusesBadLimitsAndReservedTables(t *testing.T) {
	server := startServer(t, serverConfig{})
	for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
		t.Run(string(transport), func(t *testing.T) {
			ctx := testContext(t)
			db := columnsClient(t, server.url, transport, "columns-refusals-"+string(transport))
			for _, limits := range []sirannon.ColumnLimits{{MaxRows: sirannon.Ptr(int64(-1))}, {MaxBytes: sirannon.Ptr(int64(1 << 53))}} {
				if _, err := db.QueryWithColumns(ctx, "SELECT 1", nil, limits); sirannon.ErrorCode(err) != sirannon.CodeInvalidArgument {
					t.Fatalf("limits %+v gave %v, want INVALID_ARGUMENT", limits, err)
				}
			}
			for _, statement := range []string{"SELECT * FROM '_sirannon_meta'", "SELECT * FROM [_sirannon_meta]", "SELECT * FROM main.'_sirannon_meta'"} {
				if _, err := db.QueryWithColumns(ctx, statement, nil, sirannon.ColumnLimits{}); sirannon.ErrorCode(err) != "FORBIDDEN_SQL" {
					t.Fatalf("%s gave %v, want FORBIDDEN_SQL", statement, err)
				}
				if _, err := db.Query(ctx, statement, nil); sirannon.ErrorCode(err) != "FORBIDDEN_SQL" {
					t.Fatalf("query of %s gave %v, want FORBIDDEN_SQL", statement, err)
				}
			}
			if _, err := db.QueryWithColumns(ctx, "INSERT INTO items (id) VALUES (99)", nil, sirannon.ColumnLimits{}); err == nil {
				t.Fatal("a write through QueryWithColumns succeeded")
			}
			if rows, err := db.Query(ctx, "SELECT id FROM items WHERE id = 99", nil); err != nil || len(rows) != 0 {
				t.Fatalf("after the refused write the table holds %v, %v", rows, err)
			}
		})
	}
}

func TestQueryWithColumnsRefusesAnAnswerWhoseRowsDoNotMatchItsColumns(t *testing.T) {
	bodies := []string{
		`{"columns":[],"rows":[]}`,
		`{"columns":[null],"rows":[[1]],"truncated":null}`,
		`{"columns":[{}],"rows":[[1]],"truncated":null}`,
		`{"columns":[{"name":null,"declaredType":null,"table":null,"column":null,"database":null}],"rows":[[1]],"truncated":null}`,
		`{"columns":[{"name":"a","table":null,"column":null,"database":null}],"rows":[[1]],"truncated":null}`,
		`{"columns":[{"name":"a","declaredType":null,"table":null,"column":null,"database":null}],"rows":[[1,2]],"truncated":null}`,
		`{"columns":[{"name":"a","declaredType":null,"table":null,"column":null,"database":null}],"rows":[null],"truncated":null}`,
		`{"columns":[{"name":"a","declaredType":null,"table":null,"column":null,"database":null}],"rows":[[1]],"truncated":"later"}`,
		`{"rows":[{"a":1}]}`,
	}
	for _, body := range bodies {
		fake := startFakeServer(t, func(ctx context.Context, conn *websocket.Conn, frame map[string]any) {
			sendRaw(ctx, conn, `{"type":"result","id":"`+frame["id"].(string)+`","data":`+body+`}`)
		}, func(writer http.ResponseWriter, _ *http.Request) {
			writeJSON(writer, body)
		})
		ctx := testContext(t)
		for _, transport := range []sirannon.TransportKind{sirannon.TransportWebSocket, sirannon.TransportHTTP} {
			db := columnsClient(t, fake.server.URL, transport, "hostile")
			if _, err := db.QueryWithColumns(ctx, "SELECT 1", nil, sirannon.ColumnLimits{}); sirannon.ErrorCode(err) != sirannon.CodeInvalidResponse {
				t.Fatalf("%s over %s gave %v, want INVALID_RESPONSE", body, transport, err)
			}
		}
	}
}
