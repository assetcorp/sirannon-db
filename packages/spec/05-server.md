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
  onRequest?:             OnRequestHook
  getReplicationStatus?:  () -> ReplicationStatusInfo | null
}

CorsOptions {
  origin?:  string | Array<string>
  methods?: Array<string>
  headers?: Array<string>
}
```

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Listening address |
| `port` | `9876` | Listening port |
| `cors` | disabled | CORS configuration |

### Request Hook

```text
OnRequestHook = (ctx: RequestContext) -> undefined | RequestDenial | async (undefined | RequestDenial)

RequestContext {
  headers:       Record<string, string>
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

---

## HTTP Endpoints (Normative)

All database endpoints use the path prefix `/db/{id}` where `{id}`
is the URL-encoded database identifier.

### Response Types

```text
QueryResponse {
  rows: Array<Record<string, unknown>>
}

ExecuteResponse {
  changes:         number
  lastInsertRowId: number | string
}

TransactionResponse {
  results: Array<ExecuteResponse>
}
```

The `lastInsertRowId` field is a number when it fits in a JSON
number, or a string for large 64-bit values.

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

### Error Response Format

All error responses use this format:

```json
{
  "error": {
    "code": "QUERY_ERROR",
    "message": "no such table: orders"
  }
}
```

### HTTP Status Code Mapping

| Status | Error Codes |
|--------|-------------|
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR` |
| 403 | `READ_ONLY`, `HOOK_DENIED` |
| 404 | `DATABASE_NOT_FOUND` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN` |

### Request Size Limit

The maximum HTTP request body size is 1,048,576 bytes (1 MB).
Requests exceeding this limit must be rejected with status 413 and
error code `PAYLOAD_TOO_LARGE`.

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
  type:    'subscribe'
  id:      string
  table:   string
  filter?: Record<string, unknown>
}

WSUnsubscribeMessage {
  type: 'unsubscribe'
  id:   string
}

WSQueryMessage {
  type:    'query'
  id:      string
  sql:     string
  params?: Record<string, unknown> | Array<unknown>
}

WSExecuteMessage {
  type:    'execute'
  id:      string
  sql:     string
  params?: Record<string, unknown> | Array<unknown>
}
```

### Server Messages (Outbound)

```text
WSSubscribedMessage {
  type: 'subscribed'
  id:   string
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
    row:        Record<string, unknown>
    oldRow?:    Record<string, unknown>
    seq:        string
    timestamp:  number
  }
}

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

### Payload Size

The maximum WebSocket message size is 1,048,576 bytes (1 MB),
configurable via `WSHandlerOptions.maxPayloadLength`.

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
information.

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
    "localSeq": "1547"
  }
}
```

| Field | Description |
|-------|-------------|
| `status` | `'ok'` if all databases are open; `'degraded'` if any database is closed |
| `databases` | Array of database status entries |
| `replication` | Present only when `getReplicationStatus` is configured |
| `localSeq` | String representation of the bigint sequence number |

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
