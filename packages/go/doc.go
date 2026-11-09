// Package sirannon connects a Go program to a Sirannon server over WebSocket or HTTP.
//
// [New] returns a [Client] for one server. Call [Client.Database] to get a [RemoteDatabase], which
// sends statements, registered operations, subscriptions, and live queries to one database on that
// server. The Client also has one method for every route
// that the server describes in its OpenAPI document, under the route's name in Go form, such as
// GetHealth for getHealth.
//
//	client, err := sirannon.New("http://localhost:9876", sirannon.Options{})
//	if err != nil {
//		return err
//	}
//	defer client.Close()
//	db, err := client.Database("orders")
//	if err != nil {
//		return err
//	}
//	rows, err := db.Query(ctx, "SELECT id, total FROM orders WHERE id = ?", []any{int64(42)})
//
// The client returns every integer in a row as an int64 and every BLOB as a []byte, including an
// integer beyond 2^53, so you read back each value exactly as SQLite stores it. It returns every
// failure as an [*Error], whose Code field holds the code that the server sent, or one of the Code
// constants when the client failed before the server answered.
//
// # Registered operations
//
// sirannon-codegen writes a [ReadRef] or a [WriteRef] for each operation that the server registers.
// [Query] and [Live] call a read through its ReadRef and return typed rows, while [Execute] calls a
// write through its WriteRef. A server that refuses SQL statements still accepts these calls.
//
// # Subscriptions
//
// Call [RemoteDatabase.On] and then [SubscriptionBuilder.Subscribe] to receive the changes to a table
// over the WebSocket transport. The client calls your callback once for each change, in the order of
// the server's change log, on a goroutine apart from the one that reads the connection. While 1024
// changes are waiting for your callback, the client pauses reading from the connection.
//
// When the connection drops, the client reconnects after [Options.ReconnectInterval] and resumes from
// the last change that it delivered. While the server keeps the history after that change, your
// callback receives every change exactly once. When the server has pruned that history, the client
// calls the [OnReset] handler, so read the table again there.
//
// # Replication groups
//
// [NewTopology] returns a [TopologyClient] that sends writes to the primary of a replication group
// and sends reads to the node that matches [TopologyOptions.ReadPreference]. When the client cannot
// reach the node for a read, it sets that node aside for five seconds and sends the read once more
// to another node. It sends each write once, so a write reaches the server twice only when your
// program sends it twice.
package sirannon
