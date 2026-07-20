# Sirannon Server Specification

This document defines the HTTP and WebSocket server protocol that
exposes Sirannon databases over the network. It covers endpoint
paths, request and response formats, the WebSocket subscription
protocol, and health endpoints. All Sirannon implementations that
ship a server module must follow these contracts. The endpoint
paths and message formats are normative for client-server
interoperability.

---

## Server Configuration

```text
ServerOptions {
  host?:                  string    (default: '127.0.0.1')
  port?:                  number    (default: 9876)
  cors?:                  boolean | CorsOptions
  maxBodyBytes?:          number    (default: 1_048_576)
  maxWebSocketBackpressureBytes?: number
                          (default: max(16_777_216, maxBodyBytes))
  cdcRetentionMs?:        number    (default: 3_600_000)
  onRequest?:             OnRequestHook
  resolveExecutionTarget?: (databaseId: string) ->
                            ServerExecutionTarget or null
  getReplicationStatus?:  () -> ReplicationStatusInfo or null
  getClusterStatus?:      (databaseId: string) -> ClusterStatusInfo or null
}

CorsOptions {
  origin?:  string or List<string>
  methods?: List<string>
  headers?: List<string>
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Listening address |
| `port` | `9876` | Listening port |
| `cors` | disabled | CORS configuration |
| `maxBodyBytes` | 1,048,576 | Largest accepted HTTP request body and WebSocket message, in bytes |
| `maxWebSocketBackpressureBytes` | max(16,777,216, `maxBodyBytes`) | Outbound buffer bound per WebSocket connection, in bytes |
| `cdcRetentionMs` | 3,600,000 | How long change events are retained for WebSocket subscription resumption, in milliseconds |

`maxBodyBytes` must be a positive integer. An implementation whose
transport stores the limit in a narrower type than the configured
value must refuse to start with error code `INVALID_MAX_BODY_BYTES`
rather than enforce a silently truncated limit; the reference
implementation caps the value at 4,294,967,295 for this reason.
`maxWebSocketBackpressureBytes` must be a positive integer of at
least `maxBodyBytes`, so one reply frame always fits, and is
subject to the same truncation rule with error code
`INVALID_WS_BACKPRESSURE`.

### Request Hook

```text
OnRequestHook = (ctx: RequestContext) -> null or RequestDenial or async (null or RequestDenial)

RequestContext {
  headers:       Map<string, string>
  method:        string
  path:          string
  databaseId?:   string
  remoteAddress: string
}

RequestDenial {
  status:  number
  code:    string
  message: string
}
```

The `onRequest` hook runs before every HTTP and WebSocket request.
Returning a `RequestDenial` rejects the request with the specified
status code and error body. Returning `undefined` allows the
request to proceed.

### Replication Execution Target

```text
TransactionFunction<T> = (tx: Transaction) -> async T

ServerExecutionTarget {
  query(sql: string, params?: Params, options?: QueryOptions): async -> List<Map<string, any>>
  execute(sql: string, params?: Params, options?: QueryOptions): async -> ExecuteResult
  transaction<T>(fn: TransactionFunction<T>, options?: QueryOptions): async -> T
}
```

In coordinator mode, the server must send `query`, `execute`, and
`transaction` requests to the database's replication execution
target. The target enforces primary authority checks, sync
readiness checks, forwarding, and production write concern
semantics. If the resolver returns `null`, the server responds
with `DATABASE_NOT_FOUND`.

---

## Value Encoding (Normative)

JSON cannot carry two SQLite value types without loss. JSON
numbers pass through IEEE 754 doubles, so integers outside the
range -(2^53 - 1) to 2^53 - 1 lose precision, and JSON has no
binary type. Both transports therefore wrap these values in
tagged envelopes wherever a row or a bind parameter crosses the
wire.

```text
IntegerEnvelope {
  "__sirannon_int": string
}

BlobEnvelope {
  "__sirannon_blob": string
}
```

The `__sirannon_int` payload is the exact decimal representation
of the integer: an optional leading minus sign followed by one to
nineteen digits, with no whitespace, sign prefix `+`, exponent, or
other characters. The `__sirannon_blob` payload is uppercase
hexadecimal with two digits per byte; an empty string encodes an
empty BLOB.

These rules apply to query result rows on both transports, to the
`row` and `oldRow` fields of change events, to bind parameters in
`params`, statement `params`, and `paramsBatch` entries, and to
subscription `filter` values. A filter value inside an
`IntegerEnvelope` matches rows whose column holds that exact
64-bit integer.

Producers MUST wrap an integer column value outside the safe
range in an `IntegerEnvelope` and MUST wrap every BLOB column
value in a `BlobEnvelope`. Integer values inside the safe range
are plain JSON numbers. All other value types keep their natural
JSON representation.

Consumers MUST treat a JSON object with exactly one key named
`__sirannon_int` or `__sirannon_blob`, whose value is a string,
as an envelope and decode it to the native value. Envelopes only
appear in positions where a column value is expected. A stored
TEXT value that resembles an envelope serialises as a JSON
string, never as an object, so the two cannot collide.

Servers MUST reject a malformed envelope payload in bind
parameters instead of binding it: the HTTP transport responds
with status 400 and code `INVALID_REQUEST`, and the WebSocket
transport responds with code `INVALID_MESSAGE`.

Consumers without the client SDK decode `__sirannon_int` with an
arbitrary-precision integer type, for example Python `int` or
Java `BigInteger`, and decode `__sirannon_blob` into a byte
array.

## HTTP Endpoints (Normative)

All database endpoints use the path prefix `/db/{id}` where `{id}`
is the URL-encoded database identifier.

### Response Types

```text
QueryResponse {
  rows: List<Map<string, any>>
}

ExecuteResponse {
  changes:         number
  lastInsertRowId: number or string
}

TransactionResponse {
  results: List<ExecuteResponse>
}

ClusterStatusInfo {
  databaseId:         string
  replicationGroupId?: string
  role?:             'primary' | 'replica'
  currentPrimary?:   { nodeId: string, endpoint: string } or null
  primaryTerm?:      bigint
  readEndpoints?:    List<{
    nodeId: string,
    endpoint: string,
    readConcerns: List<'local' | 'majority' | 'linearizable'>
  }>
  health:            'healthy' | 'degraded' | 'failing_over' |
                     'unavailable' | 'repairing' | 'syncing'
}
```

The `lastInsertRowId` field is a number when it fits in a JSON
number, or a string for large 64-bit values.

Values inside `rows` follow the Value Encoding rules: integers
beyond the safe range and BLOB columns arrive as tagged
envelopes.

### POST /db/{id}/query

Executes a read query and returns matching rows.

**Request:**

```json
{
  "sql": "SELECT * FROM orders WHERE status = ?",
  "params": ["pending"]
}
```

**Response (200):**

```json
{
  "rows": [
    { "id": 1, "status": "pending", "total": 42.50 }
  ]
}
```

### POST /db/{id}/execute

Executes a write statement.

**Request:**

```json
{
  "sql": "INSERT INTO orders (status, total) VALUES (?, ?)",
  "params": ["pending", 42.50]
}
```

**Response (200):**

```json
{
  "changes": 1,
  "lastInsertRowId": 7
}
```

### POST /db/{id}/transaction

Executes multiple statements as a single atomic transaction.

**Request:**

```json
{
  "statements": [
    { "sql": "UPDATE accounts SET balance = balance - ? WHERE id = ?", "params": [100, 1] },
    { "sql": "UPDATE accounts SET balance = balance + ? WHERE id = ?", "params": [100, 2] }
  ]
}
```

**Response (200):**

```json
{
  "results": [
    { "changes": 1, "lastInsertRowId": 0 },
    { "changes": 1, "lastInsertRowId": 0 }
  ]
}
```

### GET /db/{id}/cluster

Returns routing and authority metadata for the database. This
endpoint is required when coordinator mode is enabled and optional
in static mode.

**Response (200):**

```json
{
  "databaseId": "orders",
  "replicationGroupId": "orders-group",
  "role": "replica",
  "currentPrimary": {
    "nodeId": "node-a",
    "endpoint": "https://node-a.example.com/db/orders"
  },
  "primaryTerm": "42",
  "readEndpoints": [
    {
      "nodeId": "node-b",
      "endpoint": "https://node-b.example.com/db/orders",
      "readConcerns": ["local", "majority"]
    }
  ],
  "health": "healthy"
}
```

`primaryTerm` is encoded as a string to preserve 64-bit precision
in JSON. If no safe primary exists, `currentPrimary` is `null` and
the response health is `unavailable`.

### Error Response Format

All error responses use this format:

```json
{
  "error": {
    "code": "QUERY_ERROR",
    "message": "no such table: orders",
    "details": {}
  }
}
```

`details` is optional. Coordinator-mode errors should use it for
machine-readable routing context, such as `currentPrimary`,
`primaryTerm`, or `replicationGroupId`.

### HTTP Status Code Mapping

| Status | Error Codes |
|--------|-------------|
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR` |
| 403 | `READ_ONLY`, `HOOK_DENIED`, `FORBIDDEN_SQL` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH` |
| 404 | `DATABASE_NOT_FOUND` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED` |

A `WRITE_OVERLOADED` response should carry a `Retry-After` header
derived from the error's retry-after context, because the
rejection is definite load shedding and the client's correct move
is to retry after backing off. A `WRITER_WORKER_TIMEOUT` response
maps to 500 because the outcome is indeterminate; a client must
not blindly retry a non-idempotent write on it.

When a coordinator-mode server receives a write but is not the
current primary, it must either forward the write to the current
primary under the current term or reject the request with
`STALE_PRIMARY`. If it knows the current primary endpoint, the
error response should include that endpoint as structured context.

### Request Size Limit

The maximum HTTP request body size is `maxBodyBytes` (default
1,048,576 bytes). Requests exceeding this limit must be rejected
with status 413 and error code `PAYLOAD_TOO_LARGE`.

### Validation Rules

- Request body must be valid JSON.
- The `sql` field is required and must be a string.
- The `params` field must be an object or array if provided.
- Transaction requests must contain at least one statement.

---

## WebSocket Protocol (Normative)

WebSocket connections are established at `/db/{id}` where `{id}`
is the URL-encoded database identifier. The WebSocket protocol
supports queries, writes, and real-time CDC subscriptions.

### Client Messages (Inbound)

```text
WSSubscribeMessage {
  type:      'subscribe'
  id:        string
  table:     string
  filter?:   Map<string, any>
  sinceSeq?: string
  epoch?:    string
}

WSUnsubscribeMessage {
  type: 'unsubscribe'
  id:   string
}

WSQueryMessage {
  type:    'query'
  id:      string
  sql:     string
  params?: Map<string, any> or List<any>
}

WSExecuteMessage {
  type:    'execute'
  id:      string
  sql:     string
  params?: Map<string, any> or List<any>
}
```

### Server Messages (Outbound)

```text
WSSubscribedMessage {
  type:    'subscribed'
  id:      string
  seq?:    string
  epoch?:  string
  resync?: boolean
}

WSUnsubscribedMessage {
  type: 'unsubscribed'
  id:   string
}

WSChangeMessage {
  type:  'change'
  id:    string
  event: {
    type:       'insert' | 'update' | 'delete'
    table:      string
    row:        Map<string, any>
    oldRow?:    Map<string, any>
    seq:        string
    timestamp:  number
  }
}
```

Values inside `row` and `oldRow` follow the Value Encoding
rules: integers beyond the safe range and BLOB columns arrive as
tagged envelopes. The client SDK decodes them before invoking
the subscription callback; consumers without the SDK decode them
as described under Value Encoding.

```text
WSResultMessage {
  type: 'result'
  id:   string
  data: QueryResponse | ExecuteResponse
}

WSErrorMessage {
  type:  'error'
  id:    string
  error: {
    code:    string
    message: string
  }
}
```

### Message Routing

All client messages must include a string `id` field. The server
uses this `id` to correlate responses with requests. For
subscriptions, the `id` becomes the subscription identifier used
in subsequent change events and unsubscribe requests.

### Subscription Resumption

A subscription resumes across reconnects through the `sinceSeq`
and `epoch` fields. Both `sinceSeq` and `seq` are decimal
strings so sequence numbers beyond 2^53 - 1 survive JSON.

`sinceSeq` carries the highest `seq` the client has already
processed. When present, the server replays every retained
change with a greater `seq` before delivering live events, so a
reconnecting subscriber receives the changes it missed.

`epoch` identifies the sequence space a cursor was issued from.
The server reports it on the `subscribed` message; the client
stores it and echoes it when resuming. A cursor presented with a
different epoch belongs to another database, so the server
forces a resync instead of replaying unrelated rows against it.

The `seq` field of the `subscribed` message is the sequence
number the subscription is live from. A client that has not yet
received any change adopts it as its resume cursor, so a
reconnect during an idle spell still replays what it missed.

The server sets `resync: true` when the requested `sinceSeq`
fell below the retained history or arrived with a foreign epoch,
so the gap cannot be replayed. The subscription still starts
live; the client must treat its prior state as stale and re-read
the table.

### WebSocket Error Codes

| Code | Description |
|------|-------------|
| `INVALID_JSON` | Message failed JSON parsing |
| `INVALID_MESSAGE` | Missing required fields or wrong types |
| `UNKNOWN_TYPE` | Unrecognised message type |
| `DATABASE_NOT_FOUND` | Database does not exist |
| `DATABASE_CLOSED` | Database is closed |
| `HANDLER_CLOSED` | Server is shutting down |
| `PAYLOAD_TOO_LARGE` | Message exceeds maximum payload size |
| `DUPLICATE_SUBSCRIPTION` | Subscription ID already in use |
| `SUBSCRIPTION_NOT_FOUND` | Unsubscribe target does not exist |
| `READ_ONLY` | Write or subscription on read-only database |
| `CDC_UNSUPPORTED` | Subscriptions require file-based databases |
| `STALE_PRIMARY` | Request reached a stale or non-primary node |
| `COORDINATOR_UNAVAILABLE` | Coordinator authority cannot be proven |
| `AUTHORITY_LOST` | Node lost authority while handling the request |
| `NO_SAFE_PRIMARY` | No safe primary exists for the replication group |
| `READ_CONCERN_ERROR` | Requested read concern cannot be satisfied |
| `NODE_NOT_IN_SYNC` | Node is not safe for the requested read or promotion |
| `NODE_DRAINING` | Node is in maintenance drain mode |
| `WRITE_OVERLOADED` | Write shed before starting; definite and safe to retry |
| `WRITER_WORKER_TIMEOUT` | Write outcome unknown past the writer deadline's grace window |

### Payload Size

The maximum inbound WebSocket message size is `maxBodyBytes`
(default 1,048,576 bytes); one option governs both transports. A
message over the limit must be rejected with error code
`PAYLOAD_TOO_LARGE`.

### Outbound Backpressure

A subscriber that reads more slowly than its change events arrive
would otherwise buffer without bound inside the server. The server
must bound each connection's outbound buffer by
`maxWebSocketBackpressureBytes`. When a send would push a
connection's buffered outbound data past that bound, the server
must close the connection with close code 4290 rather than drop
frames silently or keep buffering; a silently dropped reply or
change event is worse than a lost connection, because the client
cannot detect it. A client that receives close code 4290 should
reconnect and resume its subscriptions through
[Subscription Resumption](#subscription-resumption), which replays
the events it missed.

### Idle Timeout

The recommended WebSocket idle timeout is 120 seconds with
automatic ping/pong keepalives.

---

## Health Endpoints

### GET /health

Liveness probe. Returns 200 if the server process is running.

**Response:**

```json
{
  "status": "ok"
}
```

### GET /health/ready

Readiness probe. Returns database status and optional replication
and cluster information.

**Response:**

```json
{
  "status": "ok",
  "databases": [
    { "id": "orders", "readOnly": false, "closed": false }
  ],
  "replication": {
    "role": "primary",
    "writeForwarding": false,
    "peers": 2,
    "localSeq": "1547",
    "replicationGroupId": "orders-group",
    "primaryTerm": "42",
    "currentPrimary": "node-a",
    "coordinator": {
      "connected": true,
      "authority": true
    },
    "controller": {
      "state": "standby"
    },
    "inSyncReplicas": ["node-b", "node-c"],
    "laggingReplicas": [],
    "syncState": "ready",
    "readAvailability": "available",
    "writeAvailability": "available"
  }
}
```

| Field | Description |
|-------|-------------|
| `status` | `'ok'`, `'degraded'`, `'failing_over'`, `'unavailable'`, `'repairing'`, or `'syncing'` |
| `databases` | Array of database status entries |
| `replication` | Present only when `getReplicationStatus` is configured |
| `localSeq` | String representation of the bigint sequence number |

Coordinator-mode readiness must distinguish these states:

- `ok`: the node can serve its advertised read and write roles;
- `degraded`: the node can serve traffic, but one or more peers
  are lagging, draining, or unavailable;
- `failing_over`: the controller is moving primary authority or
  clients must refresh routing;
- `unavailable`: the node cannot satisfy its advertised read or
  write role;
- `repairing`: the node is quarantining divergent writes or
  waiting for operator recovery;
- `syncing`: the node is copying data or catching up before it can
  serve its advertised role.

---

## CORS

When CORS is enabled, the server must handle preflight `OPTIONS`
requests and attach the appropriate headers to responses.

### Default CORS Headers

```text
Access-Control-Allow-Origin:  * (or specific origins from config)
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age:       86400
```

When the origin list is not `*`, the `Vary: Origin` header must be
included.
