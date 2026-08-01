# Sirannon Client Specification

The client SDK provides a remote database proxy over HTTP and WebSocket. It covers the client API, remote operations, subscriptions, and topology-aware routing. An implementation that ships a client module should follow these contracts. These transports connect an application to a server and are separate from the replication transport (see [04-transport.md](04-transport.md)).

---

## Client

```text
SirannonClient {
  constructor(url: string, options?: ClientOptions)

  database(id: string): RemoteDatabase
  close(): void
}

TopologyAwareClient {
  constructor(options: TopologyAwareClientOptions)

  database(id: string): RemoteDatabase
  close(): void
}

ClientOptions {
  transport?:          'websocket' | 'http'   (default: 'websocket')
  headers?:            Map<string, string>
  webSocketProtocols?: string or List<string>
  autoReconnect?:      boolean                 (default: true)
  reconnectInterval?:  number                  (default: 1000 ms)
  requestTimeout?:     number                  (default: 30000 ms, 0 waits indefinitely)
}

TopologyAwareClientOptions extends ClientOptions {
  endpoints?:      List<string>
  primary?:        string
  replicas?:       List<string>
  readPreference?: 'primary' | 'replica' | 'nearest'  (default: 'primary')
  discovery?:      'static' | 'coordinator'           (default: 'static')
  readConcern?:    'local' | 'majority' | 'linearizable'
}
```

`SirannonClient` connects to the single `url` and never routes between nodes; passing it `endpoints`, `primary`, `replicas`, `readPreference`, `discovery`, or `readConcern` fails with `INVALID_ARGUMENT`. `TopologyAwareClient` routes between the nodes of a replication group. An implementation that splits its package into entry points must place `TopologyAwareClient` behind an entry point of its own and prove that the browser-facing entry point does not reach it, so no browser bundle carries routing code or an internal node address.

`headers` applies to HTTP requests, to coordinator discovery requests, and to the WebSocket upgrade in a runtime whose WebSocket carries a handshake header. A client constructed with `headers` and the WebSocket transport in a runtime that carries none must fail at construction with `INVALID_ARGUMENT` and name `webSocketProtocols`.

A browser client carries a short-lived credential in `webSocketProtocols`. A client that configures subprotocols must offer the `sirannon.v1` identifier ahead of them, and the server selects that identifier (see [05-server.md](05-server.md#subprotocol-negotiation)). A client that configures none must offer no subprotocol.

`database(id)` returns a cached `RemoteDatabase` for the URL-encoded id. `close` closes every connection, cancels active subscriptions, and rejects pending requests.

---

## Remote Database

```text
RemoteDatabase {
  query<T>(sql, params?, options?):             async -> List<T>
  query<Row>(operation: OperationRef, args, options?): async -> List<Row>
  execute(sql, params?):                        async -> ExecuteResponse
  execute(operation: OperationRef, args, writeConcern?): async -> List<ExecuteResponse>
  live<Row>(operation: OperationRef or name, args?): async -> LiveQuery<Row>
  transaction(statements: List<{sql, params?}>): async -> List<ExecuteResponse>
  batch(sql, paramsBatch, writeConcern?):       async -> List<ExecuteResponse>
  load(sql, paramsBatch, durability?, checkpoint?): async -> BulkLoadResult
  loadAll(sql, rows, options?):                 async -> BulkLoadResult
  on(table): RemoteSubscriptionBuilder
  close(): void
}

OperationRef<Args, Row> { name: string }
```

This is a subset of the local Database API adapted for remote use: `transaction` takes a statement list rather than a callback, and `queryOne`/`executeBatch` are absent. Each method maps to the HTTP route or WebSocket message of the same name (see [05-server.md](05-server.md)). `execute` returns `lastInsertRowId` as a JSON number or a decimal string, undecoded. `loadAll` batches an iterable of parameter sets (recommended batch size 1000), sending only the final batch with `checkpoint: true`; a non-positive `batchSize` fails with `INVALID_ARGUMENT`.

A first argument that names a registered operation rather than a statement runs that operation: `query` returns its rows, and `execute` returns one result per statement of the write. An operation reference holds the name at run time, and the argument and row types of that operation at compile time, so an argument or a column the server does not serve fails to compile. An implementation without a type system passes the name as a string.

### Refusing SQL Before It Is Sent

A client must read `GET /capabilities` before it sends a statement and must fail with `SQL_NOT_ACCEPTED` when `query.sql` is absent, so that a statement a server refuses never leaves the process. This covers `query`, `execute`, `transaction`, `batch`, `load`, and `loadAll` with a statement, and never a registered operation. A client reads that answer once per server and caches it. A server that serves no `/capabilities` predates the option and accepts statements. The server also refuses on its own (see [05-server.md](05-server.md)), because a hand-written client performs no check.

### Live Queries

`live` returns the [`LiveQuery`](02-core.md#live-queries) of the local API over a registered read. The server holds the result and sends the operations that maintain it; the client applies them in order to the rows it holds. An operation with an index outside those rows must fail the query with `INVALID_RESPONSE`.

The client echoes the registry digest from `GET /capabilities` on subscribe. After `REGISTRY_MISMATCH` it re-reads the digest and subscribes once more, and fails the query if that attempt is refused. While the connection is down the query holds its rows with `revalidating` set; the transport subscribes again on reconnection, and the server sends the rows afresh. A live query requires the WebSocket transport and fails with `TRANSPORT_ERROR` over HTTP.

### Generated Operation References

A generator reads the operation registry the server is built from and writes the references a client calls it through. The types then come from the definitions the server runs, and continuous integration needs no server. For each database the generator emits the reads and writes by name and the argument names a caller supplies. Arguments the server fills from identity are absent.

The generator emits the row type of a read from its `columns`. Without them it takes the row type from the statement of a read with no arguments, and leaves the row shape open for any other read and for a select list that names no column.

The HTTP and WebSocket transports send a per-query `readConcern` to the server. The topology transport applies the client-level `readConcern` to node selection, and a transport that sends none must fail a per-query `readConcern` with `INVALID_ARGUMENT`.

---

## Client Transports

The client decodes wire values back to native representations wherever a row, change event, bind parameter, or filter crosses the wire: an `{"__sirannon_int":"<decimal>"}` envelope decodes to a 64-bit integer and an `{"__sirannon_blob":"<hex>"}` envelope to a byte array (see [value encoding](02-core.md#tagged-value-encoding-normative)). It encodes outbound integers beyond the safe range and byte arrays the same way.

### HTTP Transport

The base URL is the server URL with trailing slashes removed; requests are `application/json`. Statements map to `POST {baseUrl}/query`, `/execute`, `/transaction`, `/batch`, and `/load`, and a registered operation maps to `POST {baseUrl}/query/{name}` or `/execute/{name}`. Subscriptions and live queries are unsupported and fail with `TRANSPORT_ERROR`. A fetch failure fails with `CONNECTION_ERROR`, a non-JSON response with `INVALID_RESPONSE`, and an error response with the server's code and message.

### WebSocket Transport

The URL scheme becomes `ws://` or `wss://`. The connection is lazy; the first operation connects. It supports query, execute, transaction, batch, load, registered operations, subscriptions, and live queries. Each request carries an id of the form `c_{counter}_{timestamp}` that the server echoes. A request that exceeds `requestTimeout` fails with `TIMEOUT`. Automatic reconnection runs only while the transport has active subscriptions or live queries; a failed request of any other kind does not trigger reconnection. On reconnection every active subscription is re-established and every live query subscribes again. A live query the server refuses is dropped with that error; one whose server is unreachable stays registered for the next attempt.

A close code of 4401, 4403, or any code in the 4000-4099 range reports a refused connection. The transport must fail pending and later requests with `UNAUTHORIZED` for 4401 and `FORBIDDEN` for 4403, carry the close reason as the message, and must not reconnect. Every other close code fails pending requests with `CONNECTION_ERROR` and reconnects while subscriptions remain.

---

## Subscriptions

```text
RemoteSubscriptionBuilder {
  filter(conditions: Map<string, any>): RemoteSubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) -> void): async -> RemoteSubscription
}
RemoteSubscription { unsubscribe(): void }
```

Subscriptions require WebSocket transport; over HTTP they fail with `TRANSPORT_ERROR`. The client sends a `subscribe` message and awaits a `subscribed` confirmation, then receives `change` and `changes` messages by subscription id. It tracks the highest `seq` it has processed and the reported `epoch`, and resumes from them on reconnect, re-sending the same id and filter. A subscription that fails to restore is removed and not retried. When routing metadata changes in topology mode, active subscriptions are re-established on the new endpoint; a migrated subscription restarts live from the new endpoint rather than resuming from its prior cursor.

---

## Topology-Aware Routing

Writes (`execute`, `transaction`, `batch`, `load`) always route to the primary. Reads route by read preference.

### Static Mode

The configured `primary` and `replicas` are used directly.

| Preference | Read routing |
|------------|--------------|
| `primary` | The primary. |
| `replica` | A randomly chosen replica, or the primary when none is available. |
| `nearest` | The endpoint with the lowest measured round-trip latency, or the primary. |

For `nearest`, the client measures latency with `GET {endpoint}/health` (timeout 5,000 ms, cached 60,000 ms), treating an unreachable endpoint as unusable. A read that fails at the transport against a non-primary endpoint marks that replica removed and retries on a fallback endpoint.

### Coordinator Mode

The configured endpoints are a starter list. Before the first operation for a database, the client fetches routing metadata with `GET /db/{id}/cluster` (timeout 2,000 ms) from a reachable starter, and caches `currentPrimary`, `primaryTerm`, and the readable endpoints. A malformed response fails with `INVALID_RESPONSE`; when no endpoint yields routing, the client fails with `ROUTING_ERROR`. That endpoint serves topology only to a credential the server authorises for it (see [05-server.md](05-server.md#get-dbidcluster)), so a client configured with an application credential alone discovers nothing and fails with `ROUTING_ERROR`.

Writes go to `currentPrimary`, or fail with `NO_SAFE_PRIMARY` when none is known. The effective read concern is the per-query value, then the client-level value, then `majority`. A `linearizable` read routes to the current primary. Other reads select among readable endpoints advertising the concern: `replica` picks one at random and `nearest` picks the first readable endpoint, both falling back to the current primary and then to any endpoint advertising `local`, or failing with `ROUTING_ERROR`.

The client tracks a fingerprint of the routing metadata. A write or read that fails with `STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, or `CONNECTION_ERROR` refreshes routing; a read then retries once on the refreshed route, while a write clears its cached transport and re-raises without an automatic retry, so a non-idempotent write is never resent without the caller re-issuing it. When routing changes, active subscriptions migrate to a valid endpoint or surface a clear error.

---

## Remote Errors

```text
RemoteError { code: string, message: string }
```

A remote error carries the server's code (see [07-errors.md](07-errors.md)). Client-originated codes are `CONNECTION_ERROR`, `TIMEOUT`, `TRANSPORT_ERROR` (operation unsupported by the current transport), `INVALID_RESPONSE`, `ROUTING_ERROR`, `NO_SAFE_PRIMARY`, and `INVALID_ARGUMENT`.
