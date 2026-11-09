# Go client

`github.com/assetcorp/sirannon-db/packages/go` connects a Go 1.26 or later program to a Sirannon server over WebSocket or HTTP. It holds a typed method for every HTTP route that the server describes in `openapi.json`, so a route that the server gains appears in the client when the client is generated again. Its one dependency is `github.com/coder/websocket`.

```bash
go get github.com/assetcorp/sirannon-db/packages/go
```

```go
import sirannon "github.com/assetcorp/sirannon-db/packages/go"

client, err := sirannon.New("http://localhost:9876", sirannon.Options{})
if err != nil {
	return err
}
defer client.Close()

db, err := client.Database("orders")
if err != nil {
	return err
}
if _, err := db.Execute(ctx, "INSERT INTO orders (id, total) VALUES (?, ?)", []any{int64(42), int64(4999)}); err != nil {
	return err
}
rows, err := db.Query(ctx, "SELECT id, total FROM orders WHERE id = ?", []any{int64(42)})
```

The client returns every integer in a row as an `int64` and every BLOB as a `[]byte`, including an integer beyond 2^53, so a value reads back exactly as SQLite stores it. It sends both types the same way. `Options.Transport` takes `sirannon.TransportWebSocket`, which is the default, or `sirannon.TransportHTTP`.

## Registered operations

A server refuses statements unless it sets `acceptSql: true`, so most programs call the [registered operations](operations.md) that the server defines. `sirannon-codegen` writes a typed reference for each of them:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --go ./operations/operations.go --go-package operations
```

A Go project that has no access to the server's source can generate the same file from the manifest that the server's build writes with `--manifest`:

```bash
npx -p @delali/sirannon-db sirannon-codegen --from-manifest ./operations.json --go ./operations/operations.go
```

Each operation becomes a `sirannon.ReadRef` or a `sirannon.WriteRef` variable whose name joins the database and the operation in PascalCase, and its arguments and rows become structs with JSON tags:

```go
rows, err := sirannon.Query(ctx, db, operations.OrdersOrderByID, operations.OrdersOrderByIDArgs{ID: int64(42)})
if err != nil {
	return err
}
fmt.Println(rows[0].Total)

_, err = sirannon.Execute(ctx, db, operations.OrdersPlaceOrder, operations.OrdersPlaceOrderArgs{ID: int64(43), Total: int64(4999)})
```

The compiler refuses an argument or a column that the operation lacks. An operation with a name that a JSON tag cannot carry takes a `map[string]any` in place of a struct. When the server leaves `query.sql` out of its capabilities, the client refuses a statement with `SQL_NOT_ACCEPTED` before sending it, while every registered operation keeps working.

## Subscriptions and live queries

The WebSocket transport delivers each change to a table once and in the order of the server's change log:

```go
subscription, err := db.On("orders").
	Filter(map[string]any{"status": "pending"}).
	Subscribe(ctx, func(event sirannon.ChangeEvent) {
		fmt.Println(event.Type, event.Row, event.Seq)
	}, sirannon.OnReset(func() {
		fmt.Println("read the table again")
	}), sirannon.OnError(func(err error) {
		fmt.Println(sirannon.ErrorCode(err))
	}))
```

When the connection drops, the client reconnects every `Options.ReconnectInterval` and resumes from the last change that it delivered, so the callback receives no change twice and misses none while the server keeps the history. When the server has pruned that history, the client calls the `OnReset` handler, so read the table again in that handler. A server that refuses the connection with close code 4401 or 4403 stops the reconnection, and the client passes that error to the `OnError` handler. The client calls your callbacks one at a time, in the order that the changes arrived, on a goroutine apart from the one that reads the connection. While 1024 callbacks are waiting to run, the client stops reading from the connection until they catch up. If a slow callback holds the client back long enough, the server can close the connection with code 4290, and the client then reconnects and resumes from the last change that it delivered.

A live query keeps the rows of a registered read current as the table changes:

```go
live, err := sirannon.Live(ctx, db, operations.OrdersPendingOrders, sirannon.NoArgs{}, sirannon.OnError(func(err error) {
	fmt.Println(sirannon.ErrorCode(err))
}))
if err != nil {
	return err
}
stop := live.Subscribe(func(update sirannon.LiveUpdate[operations.OrdersPendingOrdersRow]) {
	fmt.Println(len(update.State.Rows))
})
defer stop()
```

The client applies the server's edits in the order that the server sent them. After a reconnection it subscribes again and replaces the rows with a fresh result. When a listener panics, the client recovers and passes the panic to the `OnError` handler as a `*sirannon.CallbackPanic`.

## Replication groups

`sirannon.NewTopology` sends writes to the primary of a [replication group](replication.md) and reads to the node that `ReadPreference` picks:

```go
client, err := sirannon.NewTopology(sirannon.TopologyOptions{
	Primary:        "https://db-1.example.com",
	Replicas:       []string{"https://db-2.example.com", "https://db-3.example.com"},
	ReadPreference: sirannon.ReadFromReplica,
})
```

`ReadPreference` takes `ReadFromPrimary`, `ReadFromReplica`, or `ReadFromNearest`, which measures each node's latency. With `Discovery: sirannon.DiscoveryCoordinator` and a list of `Endpoints`, the client reads the group's routing from `GET /db/{id}/cluster` and follows a failover. When a read cannot reach its node, the client sets that node aside for five seconds and runs the read once more on another node. When a write fails, the client refreshes its routing and returns the error, so a write reaches the server twice only when your program sends it twice.

## Errors

Every failure is a `*sirannon.Error` that carries the server's `Code`, `Message`, `Details`, and HTTP `Status` unchanged, so `errors.As` reaches all four and `sirannon.ErrorCode(err)` returns the code. The `sirannon.Code` constants list the codes that the client returns before it reaches a server, such as `CodeTransportError`. The [error reference](errors.md) lists the codes that the server sends.

## Routes

`*sirannon.Client` has a method for every route in `openapi.json`, which covers backups, encryption, device sync, health, and capabilities, alongside the database methods above:

```go
health, err := client.GetHealth(ctx)
```

Each method takes the route's path parameters and request body and returns a model of its response. The client has no device sync controller, so a program that syncs a device calls the device sync routes itself.
