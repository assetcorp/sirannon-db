# Sirannon Go client

`github.com/assetcorp/sirannon-db/packages/go` connects a Go 1.26 program to a Sirannon server over WebSocket or HTTP. It holds a typed method for every route in the server's OpenAPI description, and its one dependency is `github.com/coder/websocket`.

```bash
go get github.com/assetcorp/sirannon-db/packages/go
```

```go
client, err := sirannon.New("http://localhost:9876", sirannon.Options{})
if err != nil {
	return err
}
defer client.Close()

db, err := client.Database("orders")
if err != nil {
	return err
}
rows, err := db.Query(ctx, "SELECT id, total FROM orders WHERE id = ?", []any{int64(42)})
```

Every integer in a row comes back as an `int64` and every BLOB as a `[]byte`, including integers beyond 2^53, so a value reads back exactly as SQLite stores it. The client sends them the same way.

A registered operation takes typed arguments and returns typed rows:

```go
type OrderArgs struct {
	ID any `json:"id"`
}

type OrderRow struct {
	ID    any `json:"id"`
	Total any `json:"total"`
}

var orderByID = sirannon.ReadRef[OrderArgs, OrderRow]{Name: "orderById"}

orders, err := sirannon.Query(ctx, db, orderByID, OrderArgs{ID: int64(42)})
```

`db.On("orders").Subscribe(ctx, callback)` delivers changes in order. After the connection drops, it resumes from the last change that it delivered, so no change is missed or repeated while the server keeps the history after that change; once the server has pruned that history, the client calls the `OnReset` handler. `sirannon.Live` keeps the rows of a registered read current as the table changes. `sirannon.NewTopology` sends writes to the primary of a replication group and sends reads to the node that `TopologyOptions.ReadPreference` picks. It retries a read once on another node when the first node cannot answer, and it never resends a failed write.

When the server takes no SQL, a statement fails with `SQL_NOT_ACCEPTED` before it leaves the client, while registered operations keep working. Every failure comes back as a `*sirannon.Error` that carries the server's code, message, details, and HTTP status, so `errors.As` reaches all four.

## Development

`go run ./internal/generator` checks that the `generated_*.go` files match `packages/ts/openapi.json` and `packages/ts/asyncapi.json`, and `go run ./internal/generator -write` rewrites them. The tests in `tests/` start the server from `packages/ts/scripts/client-test-server.mjs`, so build `packages/ts` first, then run `go test -race -count=1 ./...` inside `tests/`.
