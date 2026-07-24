# Sirannon Server Specification

The server exposes databases over HTTP and WebSocket. Endpoint paths and message
formats are normative for client-server interoperability. Every implementation
that ships a server module must follow these contracts.

---

## Server Configuration

```text
ServerOptions {
  host?:                          string   (default: '127.0.0.1')
  port?:                          number   (default: 9876)
  cors?:                          boolean or CorsOptions
  maxBodyBytes?:                  number   (default: 1_048_576)
  maxWebSocketBackpressureBytes?: number   (default: max(16_777_216, maxBodyBytes))
  cdcRetentionMs?:                number   (default: 3_600_000)
  deviceCursorRetentionMs?:       number   (default: 2_592_000_000)   -- see 08-device-sync.md
  onRequest?:                     OnRequestHook
  resolveExecutionTarget?:        (databaseId) -> ServerExecutionTarget or null
  getReplicationStatus?:          () -> ReplicationStatusInfo or null
  getClusterStatus?:              (databaseId) -> ClusterStatusInfo or null
}

CorsOptions { origin?: string or List<string>, methods?: List<string>, headers?: List<string> }
```

`maxBodyBytes` must be a positive integer; a transport that stores the limit in a
narrower type than the configured value must refuse to start with
`INVALID_MAX_BODY_BYTES` rather than enforce a truncated limit (the reference caps
the value at 4,294,967,295). `maxWebSocketBackpressureBytes` must be a positive
integer of at least `maxBodyBytes`, so one reply frame always fits, and is subject
to the same rule with `INVALID_WS_BACKPRESSURE`.

### Request Hook

```text
OnRequestHook = (ctx: RequestContext) -> (null or RequestDenial) or async (null or RequestDenial)
RequestContext { headers: Map<string, string>, method, path, databaseId?, remoteAddress }
RequestDenial  { status: number, code: string, message: string }
```

The `onRequest` hook runs before every `/db/{id}` request: the HTTP data routes,
`GET /db/{id}/cluster`, and the WebSocket upgrade. It does not run for
`GET /health`, `GET /health/ready`, or `GET /capabilities`. Returning a
`RequestDenial` rejects the request with that status, code, and message. Returning
nothing allows it. A hook that throws rejects with status 500 and code
`HOOK_ERROR`. The server has no built-in authentication; authentication is done
in the hook.

### Execution Target

```text
ServerExecutionTarget {
  query(sql, params?, options?):        async -> List<Map<string, any>>
  execute(sql, params?, options?):      async -> ExecuteResult
  transaction(fn, options?):            async -> any
  executeTransaction?(statements, options?): async -> List<ExecuteResult>
  bulkLoad?(sql, paramsBatch, options?):     async -> BulkLoadResult
  queryForWire?(sql, params?, options?):     async -> List<Map<string, any>>
  applyChanges?(batch):                      async -> ApplyResult      -- see 08-device-sync.md
  appliedMigrations?():                      async -> List<AppliedMigration>  -- see 08-device-sync.md
}
```

The server resolves a target for each database through `resolveExecutionTarget`,
or the registry when none is configured. A resolver returning null fails with
`DATABASE_NOT_FOUND`. In coordinator mode the target enforces primary authority,
sync readiness, forwarding, and write concern. Optional members gate features: a
target without `bulkLoad` fails `/load` with `501 BULK_LOAD_UNSUPPORTED`, and one
without `applyChanges` fails `/changes` with `501 SYNC_UNSUPPORTED`.

---

## Value Encoding

Query result rows, the `row` and `oldRow` of change events, bind parameters
(`params`, statement `params`, `paramsBatch`), and subscription `filter` values
all follow the [tagged value encoding](02-core.md#tagged-value-encoding-normative):
an integer outside the safe range arrives as `{"__sirannon_int":"<decimal>"}` and a
BLOB as `{"__sirannon_blob":"<uppercase hex>"}`. A consumer without the client SDK
decodes `__sirannon_int` into an arbitrary-precision integer and `__sirannon_blob`
into a byte array. A malformed envelope in bind parameters is rejected rather than
bound: HTTP responds with `400 INVALID_REQUEST` and WebSocket with
`INVALID_MESSAGE`. A `filter` value inside an integer envelope matches rows holding
that exact 64-bit integer.

---

## HTTP Endpoints (Normative)

Database endpoints use the prefix `/db/{id}`, where `{id}` is the URL-encoded
database identifier.

| Method + path | Purpose |
|---------------|---------|
| `POST /db/{id}/query` | Run a read query |
| `POST /db/{id}/execute` | Run a write |
| `POST /db/{id}/transaction` | Run statements atomically |
| `POST /db/{id}/batch` | Run one statement over many parameter sets, atomically |
| `POST /db/{id}/load` | Bulk-load rows at relaxed durability |
| `POST /db/{id}/changes` | Apply a device-sync change batch (see [08-device-sync.md](08-device-sync.md)) |
| `POST /db/{id}/migrations` | List applied migrations (see [08-device-sync.md](08-device-sync.md)) |
| `POST /db/{id}/snapshot` | Snapshot manifest (see [08-device-sync.md](08-device-sync.md)) |
| `POST /db/{id}/snapshot/page` | Snapshot page (see [08-device-sync.md](08-device-sync.md)) |
| `GET /db/{id}/cluster` | Routing and authority metadata |
| `GET /capabilities` | Announced server capabilities (see [08-device-sync.md](08-device-sync.md)) |
| `GET /health`, `GET /health/ready` | Liveness and readiness |

Request and response bodies are JSON. `lastInsertRowId` is a JSON number when it
fits, otherwise a decimal string.

```text
POST /db/{id}/query        { sql, params?, readConcern? }        -> { rows: List<Map> }
POST /db/{id}/execute      { sql, params?, writeConcern? }       -> { changes, lastInsertRowId }
POST /db/{id}/transaction  { statements: List<{sql, params?}>, writeConcern? } -> { results: List<Execute> }
POST /db/{id}/batch        { sql, paramsBatch, writeConcern? }   -> { results: List<Execute> }
POST /db/{id}/load         { sql, paramsBatch, durability?, checkpoint? } -> { rowsLoaded, changes }
```

`readConcern` carries only `{ level }`; `writeConcern` carries `{ level, timeoutMs? }`.
A transaction needs at least one statement; a batch and a load need at least one
parameter set.

### GET /db/{id}/cluster

Returns routing metadata; required in coordinator mode, optional in static mode.
With no `getClusterStatus` configured it fails with `404 NOT_FOUND`.

```text
ClusterStatusInfo {
  databaseId:          string
  replicationGroupId?: string
  role?:               'primary' | 'replica'
  currentPrimary?:     { nodeId, endpoint } or null
  primaryTerm?:        string          -- string, to preserve 64-bit precision
  readEndpoints?:      List<{ nodeId, endpoint, readConcerns: List<'local'|'majority'|'linearizable'> }>
  health:              'healthy' | 'degraded' | 'failing_over' | 'unavailable' | 'repairing' | 'syncing'
}
```

When no safe primary exists, `currentPrimary` is null and `health` is
`unavailable`.

### Error Responses

```json
{ "error": { "code": "QUERY_ERROR", "message": "no such table: orders", "details": {} } }
```

`details` is present only when non-empty; coordinator-mode errors use it for
routing context such as `currentPrimary`, `primaryTerm`, or `serverVersion`.

### HTTP Status Codes

| Status | Codes |
|--------|-------|
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR`, `INVALID_DURABILITY`, `INVALID_SYNCHRONOUS`, `BATCH_VALIDATION_ERROR` |
| 403 | `READ_ONLY`, `FORBIDDEN_SQL`, `HOOK_DENIED` |
| 404 | `DATABASE_NOT_FOUND`, `NOT_FOUND` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH`, `MIGRATION_REQUIRED`, `SCHEMA_AHEAD` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `HOOK_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 501 | `BULK_LOAD_UNSUPPORTED`, `SYNC_UNSUPPORTED` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED` |

A code not listed defaults to 500. A `WRITE_OVERLOADED` response carries a
`Retry-After` header in seconds, because the rejection is definite load shedding.
`WRITER_WORKER_TIMEOUT` maps to 500 because its outcome is indeterminate. A
coordinator-mode server that is not the current primary either forwards the write
or rejects with `STALE_PRIMARY`, including the known primary endpoint as
structured context when it has one.

A request body over `maxBodyBytes` is rejected with `413 PAYLOAD_TOO_LARGE` before
it is fully buffered; an empty body fails with `400 EMPTY_BODY` and invalid JSON
with `400 INVALID_JSON`.

---

## WebSocket Protocol (Normative)

A WebSocket connects at `/db/{id}` and supports queries, writes, and CDC
subscriptions.

### Client Messages

```text
{ type: 'subscribe',   id, table, filter?, sinceSeq?, epoch?, deviceId?, schemaVersion? }
{ type: 'unsubscribe', id }
{ type: 'ack',         id, deviceId, seq }              -- see 08-device-sync.md
{ type: 'query',       id, sql, params? }
{ type: 'execute',     id, sql, params? }
{ type: 'transaction', id, statements, writeConcern? }
{ type: 'batch',       id, sql, paramsBatch, writeConcern? }
{ type: 'load',        id, sql, paramsBatch, durability?, checkpoint? }
```

### Server Messages

```text
{ type: 'subscribed',   id, seq?, epoch?, resync? }
{ type: 'unsubscribed', id }
{ type: 'change',       id, event: { type, table, row, oldRow?, seq, timestamp, hlc?, origin? } }
{ type: 'result',       id, data }     -- data is a query, execute, transaction, batch, load, or ack response
{ type: 'error',        id, error: { code, message } }
```

Every client message carries a string `id` the server echoes to correlate the
reply; for a subscription the `id` is the subscription identifier. `sinceSeq`,
`seq`, and `ack.seq` are decimal strings so sequence numbers beyond the safe
integer range survive JSON. Change-event `row` and `oldRow` follow the value
encoding; `hlc` and `origin` carry the change's timestamp and origin node when
stamped. The `deviceId`, `schemaVersion`, and `ack` fields drive device sync (see
[08-device-sync.md](08-device-sync.md)).

A message is rejected with `INVALID_JSON` when it is not JSON, `INVALID_MESSAGE`
when it is not an object or lacks a string `type` or `id`, and `UNKNOWN_TYPE` for
an unrecognised type. A subscription needs a string `table`; a duplicate `id`
fails with `DUPLICATE_SUBSCRIPTION`, a read-only database with `READ_ONLY`, and an
in-memory database with `CDC_UNSUPPORTED`.

### Subscription Resumption

`sinceSeq` carries the highest `seq` the client has processed. When present, the
server replays every retained change with a greater `seq` before delivering live
events. `epoch` identifies the sequence space a cursor came from; the server
reports it on `subscribed`, and a cursor presented with a different epoch forces a
resync instead of a foreign replay. The `subscribed` message's `seq` is the
sequence the subscription is live from; a client that has seen no change adopts it
as its resume cursor. The server sets `resync: true` when `sinceSeq` fell below
the retained history or arrived with a foreign epoch; the subscription still starts
live, and the client must treat its prior state as stale and re-read the table.

### Backpressure and Limits

An inbound message over `maxBodyBytes` is rejected with `PAYLOAD_TOO_LARGE`. The
server bounds each connection's outbound buffer by `maxWebSocketBackpressureBytes`;
when a send would push the buffer past the bound, the server closes the connection
with close code 4290 rather than drop a frame silently, because a dropped reply or
change event the client cannot detect is worse than a lost connection. A client
that receives 4290 should reconnect and resume through subscription resumption. The
server also closes with 1013 while shutting down, and 1008 when the database is
not found, closed, or the target resolves to none. The recommended idle timeout is
120 seconds with automatic ping/pong.

---

## Health Endpoints

`GET /health` returns `{ "status": "ok" }` while the process runs.

`GET /health/ready` returns database status and, when `getReplicationStatus` is
configured, replication status:

```json
{
  "status": "ok",
  "databases": [ { "id": "orders", "readOnly": false, "closed": false } ],
  "replication": {
    "role": "primary", "writeForwarding": false, "peers": 2, "localSeq": "1547",
    "replicationGroupId": "orders-group", "primaryTerm": "42", "currentPrimary": "node-a",
    "coordinator": { "connected": true, "authority": true },
    "controller": { "state": "standby" },
    "inSyncReplicas": ["node-b", "node-c"], "laggingReplicas": [],
    "syncState": "ready", "readAvailability": "available", "writeAvailability": "available"
  }
}
```

`localSeq` and `primaryTerm` are stringified. The readiness `status` is `ok`, or
`degraded` when a database is closed or a replica is lagging, `syncing` while a node
copies or catches up, `failing_over` while the controller is active and writes are
unavailable, and `unavailable` when both read and write are unavailable.

---

## CORS

When CORS is enabled the server answers preflight `OPTIONS` requests with
`204 No Content` and the allow headers, and attaches
`Access-Control-Allow-Origin` to responses. Defaults are origin `*`, methods
`GET, POST, OPTIONS`, headers `Content-Type, Authorization`, and
`Access-Control-Max-Age: 86400`. A string origin is echoed; a list origin is
echoed only when the request origin is listed. When the resolved origin is not
`*`, the response includes `Vary: Origin`.
