# Sirannon Client Specification

This document defines the client SDK that provides a remote
database proxy over HTTP and WebSocket transports. It covers the
client API, remote database operations, subscriptions, and
topology-aware read routing. All Sirannon implementations that
ship a client module must follow these contracts.

---

## Client

The client connects to a Sirannon server and provides access to
remote databases.

```text
SirannonClient {
  constructor(url: string, options?: ClientOptions)
  constructor(options: TopologyAwareClientOptions)

  database(id: string): RemoteDatabase
  close(): void
}
```

### ClientOptions

```text
ClientOptions {
  transport?:         'websocket' | 'http'   (default: 'websocket')
  headers?:           Map<string, string>
  autoReconnect?:     boolean                (default: true)
  reconnectInterval?: number                 (default: 1000, milliseconds)
}
```

### TopologyAwareClientOptions

```text
TopologyAwareClientOptions extends ClientOptions {
  endpoints?:      List<string>
  primary?:        string
  replicas?:       List<string>
  readPreference?: 'primary' | 'replica' | 'nearest'   (default: 'primary')
  discovery?:      'static' | 'coordinator'            (default: 'static')
  readConcern?:    'local' | 'majority' | 'linearizable'
}
```

`endpoints` is a starter list in coordinator mode. It does not
have to contain the current primary, but at least one endpoint
must be reachable for discovery to succeed.

`readConcern` is the client-level default for remote query
operations. A `QueryOptions.readConcern` value supplied to
`RemoteDatabase.query` overrides this default for that query. If
neither value is supplied, the client uses the normal server or
routing default for the selected mode.

### database(id)

Returns a `RemoteDatabase` proxy for the named database. The
database ID is URL-encoded in all transport requests.

### close()

Closes all connections and releases resources. Active subscriptions
are cancelled. Pending requests are rejected.

---

## Remote Database

The remote database proxy provides query, execute, transaction,
and subscription operations over the configured transport. Its
interface is a subset of the local Database API, adapted for
remote execution. Notable differences from the local API:
`queryOne` and `executeBatch` are not available, and `transaction`
accepts a statement array instead of a callback function.

```text
RemoteDatabase {
  query<T>(sql: string, params?: Params, options?: QueryOptions): async -> List<T>
  execute(sql: string, params?: Params): async -> ExecuteResponse
  transaction(statements: List<{ sql: string, params?: Params }>):
    async -> List<ExecuteResponse>
  on(table: string): RemoteSubscriptionBuilder
  close(): void
}
```

### query(sql, params?, options?)

Sends a query request to the server and returns the rows. Over
HTTP, this maps to `POST /db/{id}/query`. Over WebSocket, this
sends a `query` message and waits for a `result` response.
`options` uses the shared `QueryOptions` contract from
[02-core.md](02-core.md#queryoptions). For remote queries,
`readConcern` selects the required read guarantee. A per-query
`readConcern` overrides the client-level
`TopologyAwareClientOptions.readConcern`; if neither is supplied,
the client uses the normal server or routing default for the
selected mode.

### execute(sql, params?)

Sends a write request to the server. Over HTTP, this maps to
`POST /db/{id}/execute`. Over WebSocket, this sends an `execute`
message and waits for a `result` response.

### transaction(statements)

Sends a transaction request. Over HTTP, this maps to
`POST /db/{id}/transaction`. WebSocket transport does not support
transactions; implementations must throw with error code
`TRANSPORT_ERROR`.

### on(table)

Returns a subscription builder for the named table. HTTP transport
does not support subscriptions; implementations must throw with
error code `TRANSPORT_ERROR`.

---

## Subscriptions

```text
RemoteSubscriptionBuilder {
  filter(conditions: Map<string, any>): RemoteSubscriptionBuilder
  subscribe(callback: (event: ChangeEvent) -> void): async -> RemoteSubscription
}

RemoteSubscription {
  unsubscribe(): void
}
```

Subscriptions require WebSocket transport. The client sends a
`subscribe` message to the server and waits for a `subscribed`
confirmation. Change events arrive as `change` messages with the
subscription ID.

### Subscription Restoration

When the WebSocket connection drops and auto-reconnect is enabled,
the client must automatically re-establish all active subscriptions
after reconnection. The same subscription IDs and filters are
re-sent. If restoration fails for a subscription, that subscription
is removed and not retried.

---

## Transport Layer

The client supports two transport implementations. Both conform to
the same internal transport interface.

### HTTP Transport

- Base URL: the server URL with trailing slashes removed.
- Content-Type: `application/json` for all requests.
- Custom headers are merged with the default content-type header.
- Subscriptions are not supported.
- Transactions are supported.

**Endpoint Mapping:**
- `query()` -> `POST {baseUrl}/query`
- `execute()` -> `POST {baseUrl}/execute`
- `transaction()` -> `POST {baseUrl}/transaction`

**Error Handling:**
- Connection failures throw with error code `CONNECTION_ERROR`.
- Non-JSON responses throw with error code `INVALID_RESPONSE`.
- Error responses (4xx, 5xx) throw with the server's error code
  and message.

### WebSocket Transport

- URL scheme: `http://` is converted to `ws://`, `https://` to
  `wss://`.
- Connection is lazy; the first operation triggers the connect.
- Request timeout: 30,000 milliseconds (recommended).
- Subscriptions are supported.
- Transactions are not supported.

**Request ID Format:**

Each request is assigned a unique ID in the format
`c_{counter}_{timestamp}`. The server echoes this ID in its
response for correlation.

**Reconnection:**

Automatic reconnection triggers only when the transport has active
subscriptions. Non-subscription request failures do not trigger
reconnection. On successful reconnection, all active subscriptions
are re-established.

---

## Topology-Aware Routing

When configured with multiple endpoints (primary and replicas),
the client routes operations based on the read preference.

### Read Preference

In static mode, the configured `primary` and `replicas` endpoints
are used directly. In coordinator mode, these choices are made
from discovered routing metadata.

| Value | Behaviour |
|-------|----------|
| `primary` | All operations go to the primary endpoint. |
| `replica` | Reads go to a randomly selected replica. Writes go to the primary. Falls back to primary if no replicas are available. |
| `nearest` | Reads go to the endpoint with the lowest measured latency. Writes go to the primary. |

### Latency Measurement

For the `nearest` read preference, the client measures round-trip
latency to each endpoint. The recommended measurement approach:

- Timeout per endpoint: 5,000 milliseconds.
- Cache TTL: 60,000 milliseconds.
- Re-measure when the cache expires.

### Write Routing

Write operations (`execute`, `transaction`) always route to the
primary endpoint, regardless of read preference.

### Coordinator-Mode Discovery

When `discovery` is `coordinator`, the client must treat configured
endpoints as a starter list. Before the first operation for a
database, it must call `GET /db/{id}/cluster` on one reachable
starter endpoint and cache the returned `currentPrimary`,
`primaryTerm`, and readable endpoints.

Writes must be sent to the discovered current primary. If a write
fails with `STALE_PRIMARY`, `AUTHORITY_LOST`,
`COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, or a connection
failure, the client must refresh cluster metadata before retrying
when retry is safe for the operation. Non-idempotent writes must
not be retried automatically unless the runtime can prove that the
server did not commit the operation.

Reads must honour both read preference and the effective read
concern selected by the precedence rule in
[`query(sql, params?, options?)`](#querysql-params-options):

- `linearizable` reads route to the current primary and require
  live primary authority.
- `majority` reads may route to the primary or to an endpoint
  advertised for `majority` reads.
- `local` reads may route according to read preference and can
  observe stale data.

If discovery cannot find a current primary, writes fail with
`NO_SAFE_PRIMARY` or `COORDINATOR_UNAVAILABLE`. If no configured
or discovered endpoint can be reached, the client fails with
`ROUTING_ERROR`.

### Subscriptions in Coordinator Mode

Subscriptions attach to the endpoint selected for the requested
read concern. When routing metadata changes, the client must
re-establish active subscriptions on a valid endpoint or surface a
clear remote error to the subscriber.

---

## Remote Errors

```text
RemoteError {
  code:    string
  message: string
}
```

Remote errors carry the error code from the server's response. See
[07-errors.md](07-errors.md) for the full error code taxonomy.
Client-side errors (connection failures, timeouts, transport
mismatches) use these codes:

| Code | Description |
|------|-------------|
| `CONNECTION_ERROR` | Failed to connect to the server |
| `TIMEOUT` | Request exceeded the configured timeout |
| `TRANSPORT_ERROR` | Operation not supported by the current transport |
| `INVALID_RESPONSE` | Server returned a non-JSON response |
| `ROUTING_ERROR` | Client could not discover a usable primary or read endpoint |
