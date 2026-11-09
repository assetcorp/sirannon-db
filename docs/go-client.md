# Go client

`github.com/assetcorp/sirannon-db/packages/go` connects a Go 1.26 or later program to a Sirannon server over WebSocket or HTTP. It has a typed method for every HTTP route that the server describes in `openapi.json`, so when you generate the client again, it gains every route that the server has added. Its one dependency is `github.com/coder/websocket`.

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

The client returns every integer in a row as an `int64` and every BLOB as a `[]byte`, including an integer beyond 2^53, so you read back each value exactly as SQLite stores it. It sends both types the same way. `Options.Transport` takes `sirannon.TransportWebSocket`, which is the default, or `sirannon.TransportHTTP`.

## Registered operations

A server refuses statements unless it sets `acceptSql: true`, so a program that talks to such a server calls the [registered operations](operations.md) that the server defines. `sirannon-codegen` writes a typed reference for each of them:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --go ./operations/operations.go --go-package operations
```

A Go project that has no access to the server's source can generate the same file from the manifest that `sirannon-codegen --manifest` writes during the server's build:

```bash
npx -p @delali/sirannon-db sirannon-codegen --from-manifest ./operations.json --go ./operations/operations.go
```

The generator writes each operation as a `sirannon.ReadRef` or a `sirannon.WriteRef` variable, whose name is the database followed by the operation in PascalCase. It writes the arguments and rows of each operation as structs with JSON tags:

```go
rows, err := sirannon.Query(ctx, db, operations.OrdersOrderByID, operations.OrdersOrderByIDArgs{ID: int64(42)})
if err != nil {
	return err
}
fmt.Println(rows[0].Total)

_, err = sirannon.Execute(ctx, db, operations.OrdersPlaceOrder, operations.OrdersPlaceOrderArgs{ID: int64(43), Total: int64(4999)})
```

The compiler refuses an argument or a column that the operation lacks. When an argument has a name that a JSON tag cannot hold, the generator types that operation's arguments as a `map[string]any` in place of a struct, and it types the rows as a `sirannon.Row` when a column has such a name. When the server leaves `query.sql` out of its capabilities, the client refuses a statement with `SQL_NOT_ACCEPTED` before sending it, while every registered operation keeps working.

## Subscriptions and live queries

Over the WebSocket transport, the client delivers each change to a table once, in the order of the server's change log:

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

When the connection drops, the client reconnects every `Options.ReconnectInterval` and resumes from the last change that it delivered. While the server keeps the history after that change, your callback receives every change exactly once. When the server has pruned that history, the client calls the `OnReset` handler, so read the table again in that handler. When the server refuses the connection with close code 4401 or 4403, the client stops reconnecting and passes that error to the `OnError` handler.

The client calls your callbacks one at a time, in the order in which the server sent the changes, on a goroutine apart from the one that reads the connection. While 1024 callbacks are waiting, the client pauses reading from the connection until they catch up. If a slow callback keeps the client paused long enough, the server can close the connection with code 4290. The client then reconnects and resumes from the last change that it delivered.

A live query holds the rows of a registered read, and the client updates them as the table changes:

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

The client applies the server's edits in the order in which the server sent them. After a reconnection, it subscribes again and replaces the rows with a fresh result. When a listener panics, the client recovers and passes the panic to the `OnError` handler as a `*sirannon.CallbackPanic`.

## Replication groups

`sirannon.NewTopology` returns a client that sends writes to the primary of a [replication group](replication.md) and sends reads to the node that matches `ReadPreference`:

```go
client, err := sirannon.NewTopology(sirannon.TopologyOptions{
	Primary:        "https://db-1.example.com",
	Replicas:       []string{"https://db-2.example.com", "https://db-3.example.com"},
	ReadPreference: sirannon.ReadFromReplica,
})
```

`ReadPreference` takes `ReadFromPrimary`, `ReadFromReplica`, or `ReadFromNearest`, and with `ReadFromNearest` the client measures each node's latency. With `Discovery: sirannon.DiscoveryCoordinator` and a list of `Endpoints`, the client fetches the group's routing from `GET /db/{id}/cluster` and sends calls to the new primary after a failover. When the client cannot reach the node for a read, it sets that node aside for five seconds and sends the read once more to another node. When a write fails, the client returns the error without sending the write again, so a write reaches the server twice only when your program sends it twice.

## Errors

The client returns every failure as a `*sirannon.Error`, whose `Code`, `Message`, `Details`, and HTTP `Status` hold what the server sent, unchanged. `errors.As` reaches all four fields, and `sirannon.ErrorCode(err)` returns the code. The `sirannon.Code` constants list the codes that the client returns when it fails before the server answers, such as `CodeTransportError`. The [error reference](errors.md) lists the codes that the server sends.

## Routes

`*sirannon.Client` has a method for every route in `openapi.json`, which covers backups, encryption, device sync, health, and capabilities, alongside the database methods above:

```go
health, err := client.GetHealth(ctx)
```

Each method takes the route's path parameters and request body and returns a model of its response. The client has no device sync controller, so a program that syncs a device calls the device sync routes itself.
