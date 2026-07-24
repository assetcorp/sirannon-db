# Sirannon Client Specification

The client SDK provides a remote database proxy over HTTP and WebSocket. It covers
the client API, remote operations, subscriptions, and topology-aware routing. An
implementation that ships a client module should follow these contracts. These
transports connect an application to a server and are separate from the replication
transport (see [04-transport.md](04-transport.md)).

---

## Client

```text
SirannonClient {
  constructor(url: string, options?: ClientOptions)
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

Topology mode is chosen when the argument is an object carrying `primary`, a
non-empty `replicas`, a non-empty `endpoints`, or `discovery: 'coordinator'`;
otherwise the client connects to the single `url`. `headers` applies to HTTP
requests and to coordinator discovery requests. A browser WebSocket handshake
cannot attach an `Authorization` header, so a browser client passes a short-lived
credential through `webSocketProtocols` when the server validates the selected
subprotocol.

`database(id)` returns a cached `RemoteDatabase` for the URL-encoded id. `close`
closes every connection, cancels active subscriptions, and rejects pending
requests.

---

## Remote Database

```text
RemoteDatabase {
  query<T>(sql, params?, options?):             async -> List<T>
  execute(sql, params?):                        async -> ExecuteResponse
  transaction(statements: List<{sql, params?}>): async -> List<ExecuteResponse>
  batch(sql, paramsBatch, writeConcern?):       async -> List<ExecuteResponse>
  load(sql, paramsBatch, durability?, checkpoint?): async -> BulkLoadResult
  loadAll(sql, rows, options?):                 async -> BulkLoadResult
  on(table): RemoteSubscriptionBuilder
  close(): void
}
```

This is a subset of the local Database API adapted for remote use: `transaction`
takes a statement list rather than a callback, and `queryOne`/`executeBatch` are
absent. Each method maps to the HTTP route or WebSocket message of the same name
(see [05-server.md](05-server.md)). `execute` returns `lastInsertRowId` as a JSON
number or a decimal string, undecoded. `loadAll` batches an iterable of parameter
sets (recommended batch size 1000), sending only the final batch with
`checkpoint: true`; a non-positive `batchSize` fails with `INVALID_ARGUMENT`.

A per-query `readConcern` reaches the server over the HTTP transport. Over the
WebSocket and topology transports the client-level `readConcern` selects routing
and the requested guarantee.

---

## Client Transports

The client decodes wire values back to native representations wherever a row,
change event, bind parameter, or filter crosses the wire: an
`{"__sirannon_int":"<decimal>"}` envelope decodes to a 64-bit integer and an
`{"__sirannon_blob":"<hex>"}` envelope to a byte array (see
[value encoding](02-core.md#tagged-value-encoding-normative)). It encodes
outbound integers beyond the safe range and byte arrays the same way.

### HTTP Transport

The base URL is the server URL with trailing slashes removed; requests are
`application/json`. Operations map to `POST {baseUrl}/query`, `/execute`,
`/transaction`, `/batch`, and `/load`. Subscriptions are unsupported and fail with
`TRANSPORT_ERROR`. A fetch failure fails with `CONNECTION_ERROR`, a non-JSON
response with `INVALID_RESPONSE`, and an error response with the server's code and
message.

### WebSocket Transport

The URL scheme becomes `ws://` or `wss://`. The connection is lazy; the first
operation connects. It supports query, execute, transaction, batch, load, and
subscriptions. Each request carries an id of the form `c_{counter}_{timestamp}`
that the server echoes. A request that exceeds `requestTimeout` fails with
`TIMEOUT`. Automatic reconnection runs only while the transport has active
subscriptions; a failed non-subscription request does not trigger reconnection. On
reconnection every active subscription is re-established.

---

## Subscriptions

```text
RemoteSubscriptionBuilder {
  filter(conditions: Map<string, any>): RemoteSubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) -> void): async -> RemoteSubscription
}
RemoteSubscription { unsubscribe(): void }
```

Subscriptions require WebSocket transport; over HTTP they fail with
`TRANSPORT_ERROR`. The client sends a `subscribe` message and awaits a `subscribed`
confirmation, then receives `change` messages by subscription id. It tracks the
highest `seq` it has processed and the reported `epoch`, and resumes from them on
reconnect, re-sending the same id and filter. A subscription that fails to restore
is removed and not retried. When routing metadata changes in topology mode, active
subscriptions are re-established on the new endpoint; a migrated subscription
restarts live from the new endpoint rather than resuming from its prior cursor.

---

## Topology-Aware Routing

Writes (`execute`, `transaction`, `batch`, `load`) always route to the primary.
Reads route by read preference.

### Static Mode

The configured `primary` and `replicas` are used directly.

| Preference | Read routing |
|------------|--------------|
| `primary` | The primary. |
| `replica` | A randomly chosen replica, or the primary when none is available. |
| `nearest` | The endpoint with the lowest measured round-trip latency, or the primary. |

For `nearest`, the client measures latency with `GET {endpoint}/health` (timeout
5,000 ms, cached 60,000 ms), treating an unreachable endpoint as unusable. A read
that fails at the transport against a non-primary endpoint marks that replica
removed and retries on a fallback endpoint.

### Coordinator Mode

The configured endpoints are a starter list. Before the first operation for a
database, the client fetches routing metadata with `GET /db/{id}/cluster` (timeout
2,000 ms) from a reachable starter, and caches `currentPrimary`, `primaryTerm`,
and the readable endpoints. A malformed response fails with `INVALID_RESPONSE`;
when no endpoint yields routing, the client fails with `ROUTING_ERROR`.

Writes go to `currentPrimary`, or fail with `NO_SAFE_PRIMARY` when none is known.
The effective read concern is the per-query value, then the client-level value,
then `majority`. A `linearizable` read routes to the current primary. Other reads
select among readable endpoints advertising the concern: `replica` picks one at
random and `nearest` picks the first readable endpoint, both falling back to the
current primary and then to any endpoint advertising `local`, or failing with
`ROUTING_ERROR`.

The client tracks a fingerprint of the routing metadata. A write or read that
fails with `STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`,
`NO_SAFE_PRIMARY`, or `CONNECTION_ERROR` refreshes routing; a read then retries
once on the refreshed route, while a write clears its cached transport and
re-raises without an automatic retry, so a non-idempotent write is never resent
without the caller re-issuing it. When routing changes, active subscriptions
migrate to a valid endpoint or surface a clear error.

---

## Remote Errors

```text
RemoteError { code: string, message: string }
```

A remote error carries the server's code (see [07-errors.md](07-errors.md)).
Client-originated codes are `CONNECTION_ERROR`, `TIMEOUT`, `TRANSPORT_ERROR`
(operation unsupported by the current transport), `INVALID_RESPONSE`,
`ROUTING_ERROR`, `NO_SAFE_PRIMARY`, and `INVALID_ARGUMENT`.
