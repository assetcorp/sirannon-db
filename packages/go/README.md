# Sirannon Go client

`github.com/assetcorp/sirannon-db/packages/go` connects a Go 1.26 program to a Sirannon server over WebSocket or HTTP. The package has a typed method for every route in the server's OpenAPI description, and it depends on one module, `github.com/coder/websocket`.

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

The client returns every integer in a row as an `int64` and every BLOB as a `[]byte`, including integers beyond 2^53, so each value matches what SQLite stores. It encodes `int64` and `[]byte` arguments the same way when it sends them.

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

`db.On("orders").Subscribe(ctx, callback)` delivers changes in order. When the connection drops, the subscription resumes from the last change that it delivered. While the server keeps the history after that change, the subscription delivers each change once; once the server prunes that history, the client calls the `OnReset` handler. `sirannon.Live` updates the rows of a registered read as the table changes.

The client from `sirannon.NewTopology` sends writes to the primary of a replication group and sends reads to the node that `TopologyOptions.ReadPreference` selects. When a read fails because its node is down or the group's routing has changed, the client retries the read once on the node that its updated routing selects. It never resends a failed write.

When the server accepts no SQL, the client refuses a statement with `SQL_NOT_ACCEPTED` before it sends anything, while registered operations work as usual. The client returns every failure as a `*sirannon.Error` with the server's code, message, details, and HTTP status, so `errors.As` gives you all four.

## Development

`go run ./internal/generator` checks that the `generated_*.go` files match `packages/ts/openapi.json` and `packages/ts/asyncapi.json`, and `go run ./internal/generator -write` rewrites them. The tests in `tests/` start the server from `packages/ts/scripts/client-test-server.mjs`, so build `packages/ts` first, then run `go test -race -count=1 ./...` inside `tests/`.
