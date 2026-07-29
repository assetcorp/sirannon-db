# Sirannon Server Specification

The server exposes databases over HTTP and WebSocket. Endpoint paths and message formats are normative for client-server interoperability. Every implementation that ships a server module must follow these contracts.

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
  maxUnacknowledgedChanges?:      number   (default: 1_000)           -- see 08-device-sync.md
  authenticate?:                  AuthenticateHook
  operations?:                    OperationRegistry
  acceptSql?:                     boolean  (default: false)
  resolveExecutionTarget?:        (databaseId) -> ServerExecutionTarget or null
  getReplicationStatus?:          () -> ReplicationStatusInfo or null
  getClusterStatus?:              (databaseId) -> ClusterStatusInfo or null
  authorizeClusterStatus?:        ClusterStatusAuthorizer
}

CorsOptions { origin?: string or List<string>, methods?: List<string>, headers?: List<string> }
```

`maxBodyBytes` must be a positive integer; a transport that stores the limit in a narrower type than the configured value must refuse to start with `INVALID_MAX_BODY_BYTES` rather than enforce a truncated limit (the reference caps the value at 4,294,967,295). `maxWebSocketBackpressureBytes` must be a positive integer of at least `maxBodyBytes`, so one reply frame always fits, and is subject to the same rule with `INVALID_WS_BACKPRESSURE`.

### Authentication

```text
AuthenticateHook = (ctx: RequestContext) -> Identity or null, or async of either
RequestContext   { headers: Map<string, string>, method, path, databaseId?, remoteAddress }
```

The server must invoke `authenticate` before every `/db/{id}` request: the HTTP data routes, `GET /db/{id}/cluster`, and the WebSocket upgrade. It must not invoke the hook for `GET /health`, `GET /health/ready`, or `GET /capabilities`. The hook returns the identity for the request, or nothing for an anonymous request. To reject a request, the hook throws. An error with a `status` property must produce that status, code, and message; any other error must produce status 500 with code `HOOK_ERROR`. Sirannon defines no built-in authentication, so the hook is where an implementation authenticates a request.

### Registered Operations

```text
OperationRegistry = Map<databaseId, {
  reads?:  Map<name, { args?: List<string>, fromIdentity?: Map<string, identityField>,
                       columns?: List<string>, statement: (args) -> Statement }>
  writes?: Map<name, { args?: List<string>, fromIdentity?: Map<string, identityField>,
                       statements: (args) -> Statement or List<Statement> }>
}>

Statement { sql: string, params?: Params }
```

A caller invokes a registered operation by name, and the request carries no SQL. The registry is server-side code, keyed by database identifier. `args` declares the argument names a caller may supply. `fromIdentity` maps an argument name to a field of the identity `authenticate` returned, and the server must supply that argument itself. A request supplying an argument named in `fromIdentity` must fail with `ARGUMENT_NOT_ALLOWED`; the server must not override the supplied value instead. An implementation must constrain `fromIdentity` values to the fields of the identity type, so that a wrong field name fails to compile.

A read contains exactly one statement and an optional `columns` list of what that statement returns. A write contains one or more statements, and the server must run them in a single transaction. The server passes the resolved arguments to both. It serves them over HTTP at `POST /db/{id}/query/{name}` and `POST /db/{id}/execute/{name}`, over WebSocket through a `query` or an `execute` message carrying `name`, and opens a live query over a read when a `subscribe` message names one.

A server configured with a registry must announce `query.named` through `GET /capabilities` and include `registry.digest`, a hash over every registered database identifier, operation kind, operation name, and argument name. The digest must change whenever the contract a client generates against changes, which is how a client detects a rolling deploy. A server that accepts SQL statements over the network must announce `query.sql`, and a server that rejects them must omit the token, because a client tests for its absence before sending SQL.

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

The server resolves a target for each database through `resolveExecutionTarget`, or the registry when none is configured. A resolver returning null fails with `DATABASE_NOT_FOUND`. In coordinator mode the target enforces primary authority, sync readiness, forwarding, and write concern. Optional members gate features: a target without `bulkLoad` fails a served `/load` with `501 BULK_LOAD_UNSUPPORTED`, and one without `applyChanges` fails `/changes` with `501 SYNC_UNSUPPORTED`.

---

## Value Encoding

Query result rows, the `row` and `oldRow` of change events, bind parameters (`params`, statement `params`, `paramsBatch`), and subscription `filter` values all follow the [tagged value encoding](02-core.md#tagged-value-encoding-normative): an integer outside the safe range arrives as `{"__sirannon_int":"<decimal>"}` and a BLOB as `{"__sirannon_blob":"<uppercase hex>"}`. A consumer without the client SDK decodes `__sirannon_int` into an arbitrary-precision integer and `__sirannon_blob` into a byte array. A malformed envelope in bind parameters is rejected rather than bound: HTTP responds with `400 INVALID_REQUEST` and WebSocket with `INVALID_MESSAGE`. A `filter` value inside an integer envelope matches rows holding that exact 64-bit integer.

---

## HTTP Endpoints (Normative)

Database endpoints use the prefix `/db/{id}`, where `{id}` is the URL-encoded database identifier.

| Method + path | Purpose |
|---------------|---------|
| `POST /db/{id}/query` | Run a read query |
| `POST /db/{id}/query/{name}` | Run a registered read |
| `POST /db/{id}/execute` | Run a write |
| `POST /db/{id}/execute/{name}` | Run a registered write |
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

Request and response bodies are JSON. `lastInsertRowId` is a JSON number when it fits, otherwise a decimal string.

```text
POST /db/{id}/query        { sql, params?, readConcern? }        -> { rows: List<Map> }
POST /db/{id}/execute      { sql, params?, writeConcern? }       -> { changes, lastInsertRowId }
POST /db/{id}/transaction  { statements: List<{sql, params?}>, writeConcern? } -> { results: List<Execute> }
POST /db/{id}/batch        { sql, paramsBatch, writeConcern? }   -> { results: List<Execute> }
POST /db/{id}/load         { sql, paramsBatch, durability?, checkpoint? } -> { rowsLoaded, changes }

POST /db/{id}/query/{name}   { args?, readConcern? }   -> { rows: List<Map> }
POST /db/{id}/execute/{name} { args?, writeConcern? }  -> { results: List<Execute> }
```

`acceptSql` governs the five statement routes and the five statement WebSocket messages, and defaults to false. With it false, a server must serve registered operations only, and must fail `POST /db/{id}/query`, `/execute`, `/transaction`, `/batch`, and `/load`, and a `query`, `execute`, `transaction`, `batch`, or `load` message, with `SQL_NOT_ACCEPTED`. A path that matches no route must still fail with `NOT_FOUND`, so that a caller distinguishes a refused capability from a wrong address.

`{name}` is URL-encoded, and the server must decode it before matching. `args` follows the value encoding, and an operation declaring no argument accepts an empty body. A name registered as neither a read nor a write must fail with `UNKNOWN_QUERY` on both routes, and the write route must not resolve a read name. `execute/{name}` returns one result per statement.

`readConcern` carries only `{ level }`; `writeConcern` carries `{ level, timeoutMs? }`. A transaction needs at least one statement; a batch and a load need at least one parameter set.

### GET /db/{id}/cluster

Returns routing metadata; required in coordinator mode, optional in static mode. The metadata names the address of every node in the group, so the endpoint answers only a request that `authorizeClusterStatus` accepts.

```text
ClusterStatusAuthorizer = (ctx: RequestContext) -> boolean or async boolean
```

The `authenticate` hook runs first, as it does for every `/db/{id}` request. The endpoint then fails with `404 NOT_FOUND` when `getClusterStatus` is not configured, when `authorizeClusterStatus` is not configured, or when `authorizeClusterStatus` returns false. The three responses are identical, so a caller cannot tell a refusal from a server that runs no cluster. An authorisation hook that throws fails with `500 HOOK_ERROR`. An accepted request for an unknown database fails with `404 DATABASE_NOT_FOUND`.

Grant this permission to an operator or to another node. An application credential must not hold it, because the node map exposes internal addresses a client can then reach directly.

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

When no safe primary exists, `currentPrimary` is null and `health` is `unavailable`.

### Error Responses

```json
{ "error": { "code": "QUERY_ERROR", "message": "no such table: orders", "details": {} } }
```

`details` is present only when non-empty; coordinator-mode errors use it for routing context such as `currentPrimary`, `primaryTerm`, or `serverVersion`.

### HTTP Status Codes

| Status | Codes |
|--------|-------|
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR`, `INVALID_DURABILITY`, `INVALID_SYNCHRONOUS`, `BATCH_VALIDATION_ERROR`, `MISSING_ARGUMENT`, `ARGUMENT_NOT_ALLOWED` |
| 401 | `IDENTITY_REQUIRED` |
| 403 | `READ_ONLY`, `FORBIDDEN_SQL`, `HOOK_DENIED`, `SQL_NOT_ACCEPTED` |
| 404 | `DATABASE_NOT_FOUND`, `NOT_FOUND`, `UNKNOWN_QUERY` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH`, `MIGRATION_REQUIRED`, `SCHEMA_AHEAD`, `REGISTRY_MISMATCH` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `HOOK_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 501 | `BULK_LOAD_UNSUPPORTED`, `SYNC_UNSUPPORTED` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED` |

A code not listed defaults to 500, and an error carrying an explicit status uses it, which is how `authenticate` rejects with a status of its own. A `WRITE_OVERLOADED` response carries a `Retry-After` header in seconds, because the rejection is definite load shedding. `WRITER_WORKER_TIMEOUT` maps to 500 because its outcome is indeterminate. A coordinator-mode server that is not the current primary either forwards the write or rejects with `STALE_PRIMARY`, including the known primary endpoint as structured context when it has one.

A request body over `maxBodyBytes` is rejected with `413 PAYLOAD_TOO_LARGE` before it is fully buffered; an empty body fails with `400 EMPTY_BODY` and invalid JSON with `400 INVALID_JSON`.

---

## WebSocket Protocol (Normative)

A WebSocket connects at `/db/{id}` and supports queries, writes, and CDC subscriptions.

### Client Messages

```text
{ type: 'subscribe',   id, table, tables?, filter?, sinceSeq?, epoch?, deviceId?, schemaVersion? }
{ type: 'subscribe',   id, name, args?, registryDigest? }        -- a live query
{ type: 'unsubscribe', id }
{ type: 'ack',         id, deviceId, seq }              -- see 08-device-sync.md
{ type: 'query',       id, sql, params?, readConcern? }
{ type: 'query',       id, name, args?, readConcern? }
{ type: 'execute',     id, sql, params? }
{ type: 'execute',     id, name, args?, writeConcern? }
{ type: 'transaction', id, statements, writeConcern? }
{ type: 'batch',       id, sql, paramsBatch, writeConcern? }
{ type: 'load',        id, sql, paramsBatch, durability?, checkpoint? }
```

A `query` or an `execute` message carrying `name` runs the registered read or write of that name and carries no SQL, so `acceptSql` does not govern it. `args` follows the value encoding and resolves as it does on the HTTP routes, against the identity the `authenticate` hook returned for the upgrade request. A registered write replies with one result per statement.

The server applies the `readConcern` of a `query` message to the read it runs, and must fail an invalid `readConcern` with `INVALID_MESSAGE`.

### Server Messages

```text
{ type: 'subscribed',   id, seq?, epoch?, resync?, rows? }
{ type: 'unsubscribed', id }
{ type: 'change',       id, event: { type, table, row, oldRow?, seq, timestamp, hlc?, origin?, rowId?, txId?, txEnd? } }
{ type: 'live',         id, ops?, rows?, revalidating? }
{ type: 'result',       id, data }     -- data is a query, execute, transaction, batch, load, or ack response
{ type: 'error',        id, error: { code, message } }
```

Every client message carries a string `id` the server echoes to correlate the reply; for a subscription the `id` is the subscription identifier. `sinceSeq`, `seq`, and `ack.seq` are decimal strings so sequence numbers beyond the safe integer range survive JSON. Change-event `row` and `oldRow` follow the value encoding, and `rowId` identifies the changed row. `hlc`, `origin`, and `txId` carry the change's timestamp, origin node, and transaction when it is stamped, and `txEnd` is true on the last change of a transaction (see [Transaction Boundaries](#transaction-boundaries)). The `deviceId`, `schemaVersion`, and `ack` fields drive device sync (see [08-device-sync.md](08-device-sync.md)).

A message is rejected with `INVALID_JSON` when it is not JSON, `INVALID_MESSAGE` when it is not an object or lacks a string `type` or `id`, and `UNKNOWN_TYPE` for an unrecognised type. A subscription needs a string `table`, or a `tables` array of 1 to 500 table names in place of it; `tables` requires a `deviceId`. A duplicate `id` fails with `DUPLICATE_SUBSCRIPTION`, a read-only database with `READ_ONLY`, and an in-memory database with `CDC_UNSUPPORTED`.

### Transaction Boundaries

Every subscription marks the last change of each transaction with `txEnd`, so a consumer applies a whole transaction at once and never shows a state the database never held. Subscribing turns on write stamping for the database, because a change carries no `tx_id` until the server stamps it (see [08-device-sync.md](08-device-sync.md)).

The last change of a transaction is identifiable only once the following change arrives, so the server must hold one change per subscription and release it when a change of another transaction arrives or the poll reaches a transaction boundary. The held change is one change, whatever the size of the transaction. A poll ending part-way through a transaction is not a boundary, and the server must keep holding that change until the rest of the transaction arrives. A change carrying no `txId` forms its own transaction, and the server must deliver it marked.

The server must apply the filter before the marker, so `txEnd` marks the last change to pass the filter rather than the last change of the transaction, and a transaction whose changes all fail the filter delivers nothing. Replayed history carries the same marker.

### Live Queries

A subscription naming a registered read is a live query. The server opens the live query of [02-core.md](02-core.md#live-queries) over the statement that read returns, answers `subscribed` with `rows`, and sends a `live` message for each later change to the result. The rows are part of `subscribed`. A separate read leaves a gap, because the server consumes changes between the two messages.

A `live` message carries exactly one of three fields. `ops` is a list of `{ op: 'insert' | 'update' | 'delete', index, row? }`, applied in order to the rows the client holds, and one message carries one transaction. `rows` replaces the held rows after a second read. `revalidating: true` states that a second read is running and that the held rows are the last complete answer.

A live subscription carries no `table`, `tables`, `filter`, `sinceSeq`, `epoch`, `deviceId`, or `schemaVersion`; a message carrying one must fail with `INVALID_MESSAGE`. The server holds the result, so a client resumes by subscribing again rather than from a cursor. `unsubscribe` closes the live query and drops its probe table. Row values follow the value encoding.

`registryDigest` echoes `registry.digest` from `GET /capabilities`. A server whose digest differs must fail with `REGISTRY_MISMATCH`. A client must then re-read `/capabilities` once and subscribe again with the digest it returns.

### Subscription Resumption

`sinceSeq` carries the highest `seq` the client has processed. When present, the server replays every retained change with a greater `seq` before delivering live events. `epoch` identifies the sequence space a cursor came from; the server reports it on `subscribed`, and a cursor presented with a different epoch forces a resync instead of a foreign replay. The `subscribed` message's `seq` is the sequence the subscription is live from; a client that has seen no change adopts it as its resume cursor. The server sets `resync: true` when `sinceSeq` fell below the retained history or arrived with a foreign epoch; the subscription still starts live, and the client must treat its prior state as stale and re-read the table.

### Backpressure and Limits

An inbound message over `maxBodyBytes` is rejected with `PAYLOAD_TOO_LARGE`. The server bounds each connection's outbound buffer by `maxWebSocketBackpressureBytes`; when a send would push the buffer past the bound, the server must close the connection with close code 4290 rather than drop a frame, so that the client detects the loss. A client that receives 4290 should reconnect and resume through subscription resumption. The server also closes with 1013 while shutting down, and 1008 when the database is not found, closed, or the target resolves to none. The recommended idle timeout is 120 seconds with automatic ping/pong. A subscription presenting a `deviceId` is also paced by acknowledgements: the server holds delivery once the highest sequence sent runs more than `maxUnacknowledgedChanges` ahead of that device's acknowledged cursor, and resumes on the next `ack`.

---

## Health Endpoints

`GET /health` returns `{ "status": "ok" }` while the process runs.

`GET /health/ready` returns database status and, when `getReplicationStatus` is configured, replication status:

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

`localSeq` and `primaryTerm` are stringified. The readiness `status` is `ok`, or `degraded` when a database is closed or a replica is lagging, `syncing` while a node copies or catches up, `failing_over` while the controller is active and writes are unavailable, and `unavailable` when both read and write are unavailable.

---

## CORS

When CORS is enabled the server answers preflight `OPTIONS` requests with `204 No Content` and the allow headers, and attaches `Access-Control-Allow-Origin` to responses. Defaults are origin `*`, methods `GET, POST, OPTIONS`, headers `Content-Type, Authorization`, and `Access-Control-Max-Age: 86400`. A string origin is echoed; a list origin is echoed only when the request origin is listed. When the resolved origin is not `*`, the response includes `Vary: Origin`.
