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
  acceptBackupRestore?:           boolean  (default: false)
  acceptEncryptionControl?:       boolean  (default: false)
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

The server must invoke `authenticate` before every `/db/{id}` request: the HTTP data routes, the backup routes, `GET /db/{id}/cluster`, and the WebSocket upgrade. It must not invoke the hook for `GET /health`, `GET /health/ready`, or `GET /capabilities`. The hook returns the identity for the request, or nothing for an anonymous request. To reject a request, the hook throws. An error with a `status` property must produce that status, code, and message; any other error must produce status 500 with code `HOOK_ERROR`. Sirannon defines no built-in authentication, so the hook is where an implementation authenticates a request.

On the WebSocket upgrade the server must complete the handshake and close immediately with code 4401 for a refusal of status 401 and 4403 for a refusal of status 403, carrying the error code and message as the close reason. A refusal of any other status must produce the HTTP status response.

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
| `POST /db/{id}/backup` | Run one turn of the checkpoint cycle |
| `GET /db/{id}/backup` | Cycle status and the progress of the run in flight |
| `GET /db/{id}/backup/chain` | List what the backup destination stores |
| `POST /db/{id}/backup/verify` | Read one stored backup back and check it |
| `POST /db/{id}/backup/safe-to-delete` | List the records no restore still needs |
| `POST /db/{id}/backup/restore` | Rebuild the database from a moment |
| `GET /db/{id}/backup/restore` | How that restore went |
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
  health:              NodeHealth.state
  healthReason:        NodeHealth.reason
}
```

`health` and `healthReason` carry `NodeHealth`, defined in [03-replication.md](03-replication.md). When no safe primary exists, `currentPrimary` is null and `health` is `unavailable`.

`readEndpoints` holds one entry per node that counts towards majority and is neither quarantined, draining, nor repairing. A node the group counts as in sync serves `local` and `majority`; a node that has fallen behind serves `local` alone, because a `local` read carries no in-sync requirement. A node running without a coordinator omits `readEndpoints`.

### Backup Endpoints

The server serves the backup routes to an operator. It runs `authenticate` before each of them, as before every `/db/{id}` request, and defines no separate authorisation hook: `RequestContext` states `method` and `path`, so one hook accepts an identity on the data routes and refuses it here.

```text
POST /db/{id}/backup                                             -> 202 { started: true }
GET  /db/{id}/backup                                             -> BackupCycleStatus
GET  /db/{id}/backup/chain                                       -> { chains: List<BackupChain> }
POST /db/{id}/backup/verify          { name }                    -> BackupVerifyResult
POST /db/{id}/backup/safe-to-delete  { restorableFrom? }         -> { records: List<BackupChainRecord> }
POST /db/{id}/backup/restore         { moment?, batchSize? }     -> 202 { started: true }
GET  /db/{id}/backup/restore                                     -> BackupRestoreStatus
```

[02-core.md](02-core.md) defines `BackupCycleStatus`, `BackupVerifyResult`, `BackupChain`, `BackupChainRecord`, `BackupRestoreProgress`, and `BackupRestoreReport`.

A body whose every field is optional may be empty. A missing `name`, a `moment` or `restorableFrom` below zero or fractional, a `batchSize` outside one to 4096, and a body that parses as JSON but is not an object each fail with `400 INVALID_REQUEST`.

Every route but `GET /db/{id}/backup/restore` addresses an open database. An identifier with none open fails with `404 DATABASE_NOT_FOUND`, and a database opened without `backups` fails with `501 BACKUP_UNSUPPORTED`.

The server answers `POST /db/{id}/backup` with `202` once it has accepted the turn, and must not wait for that turn. `GET /db/{id}/backup` reports the outcome. A turn triggered while another is under way queues behind it, and every turn triggered before that queued turn begins joins it, so at most one turn waits.

`acceptBackupRestore` governs `POST /db/{id}/backup/restore` and defaults to false. With it false, the server fails that route with `403 BACKUP_RESTORE_NOT_ACCEPTED` before it resolves the database. A server configured with `acceptBackupRestore` true and no `authenticate` hook refuses to start, with `INVALID_BACKUP_RESTORE`.

A restore rebuilds the database at the path it already occupies, from the chain that database's own backups form. It applies the `destinationTimeoutMs` the operator opened that database with. The server closes the database, which captures its log a final time; discards the cycle state and the staged captures of the chain it was extending; rebuilds the file; and opens the database again under the same identifier with the settings it had. That order is normative: an implementation discards the state before it replaces the file, so that no process resumes capturing onto a chain a restore has replaced.

The server answers `404 DATABASE_NOT_FOUND` for that identifier on every route while the rebuild proceeds. The first turn after the reopen starts a fresh chain with a full copy. A rebuild that fails still opens the database again. A close that fails leaves nothing open under the identifier. A reopen that fails after a rebuild succeeded reports `done`, with the report and a `reopenError`.

The server answers `POST /db/{id}/backup/restore` with `202` and must not wait. A second restore of the same database while one is under way fails with `409 BACKUP_RESTORE_IN_PROGRESS`.

```text
BackupRestoreStatus {
  state:        'idle' | 'running' | 'done' | 'failed'
  moment?:      number
  startedAt?:   number
  finishedAt?:  number
  progress?:    BackupRestoreProgress
  report?:      BackupRestoreReport
  error?:       { code: string, message: string }
  reopenError?: { code: string, message: string }
}
```

`GET /db/{id}/backup/restore` reads the server's own record, so it answers while that database is closed. It reports `idle` where no restore has started since the server did, and the server keeps one record per identifier, replaced by each new restore. `error` and `reopenError` state a message only where a `SirannonError` supplied one.

### Encryption Endpoints

The server serves the encryption routes to an operator, under the `authenticate` hook every `/db/{id}` request runs.

```text
POST /db/{id}/encryption          { target, dataKey? }  -> 202 { started: true }
GET  /db/{id}/encryption                                -> EncryptionStatus
POST /db/{id}/encryption/rotate   { masterKeyName }     -> EncryptionStatus
POST /db/{id}/encryption/suspend                        -> 202 { started: true }
POST /db/{id}/encryption/resume                         -> 202 { started: true }
```

```text
EncryptionStatus {
  encrypted:      boolean
  masterKeyName?: string
  rotatedAt?:     number
  job?:           ReencryptStatus
}
```

[02-core.md](02-core.md) defines `ReencryptRequest` and `ReencryptStatus`. Every response states the master key by its name alone.

`acceptEncryptionControl` governs every route above and defaults to false. With it false, the server fails those routes with `403 ENCRYPTION_CONTROL_NOT_ACCEPTED` before it resolves the database. A server configured with `acceptEncryptionControl` true and no `authenticate` hook refuses to start, with `INVALID_ENCRYPTION_CONTROL`.

The server answers `POST /db/{id}/encryption` with `202` once it has accepted the job, and `GET /db/{id}/encryption` reports the outcome. The server answers `404 DATABASE_NOT_FOUND` for that identifier while the job holds the database offline for its swap. `POST /db/{id}/encryption/rotate` re-wraps the key record and answers once it has done so, because that write touches one page.

### Error Responses

```json
{ "error": { "code": "QUERY_ERROR", "message": "no such table: orders", "details": {} } }
```

`details` is present only when non-empty; coordinator-mode errors use it for routing context such as `currentPrimary`, `primaryTerm`, or `serverVersion`.

### HTTP Status Codes

| Status | Codes |
|--------|-------|
| 400 | `INVALID_REQUEST`, `INVALID_JSON`, `EMPTY_BODY`, `QUERY_ERROR`, `TRANSACTION_ERROR`, `INVALID_DURABILITY`, `INVALID_SYNCHRONOUS`, `BATCH_VALIDATION_ERROR`, `MISSING_ARGUMENT`, `ARGUMENT_NOT_ALLOWED`, `UNSUPPORTED_SUBPROTOCOL` |
| 401 | `IDENTITY_REQUIRED` |
| 403 | `READ_ONLY`, `FORBIDDEN_SQL`, `HOOK_DENIED`, `SQL_NOT_ACCEPTED`, `BACKUP_RESTORE_NOT_ACCEPTED`, `ENCRYPTION_CONTROL_NOT_ACCEPTED` |
| 404 | `DATABASE_NOT_FOUND`, `NOT_FOUND`, `UNKNOWN_QUERY` |
| 409 | `STALE_PRIMARY`, `PROTOCOL_VERSION_MISMATCH`, `MIGRATION_REQUIRED`, `SCHEMA_AHEAD`, `REGISTRY_MISMATCH`, `BACKUP_CHAIN_BROKEN`, `BACKUP_RESTORE_IN_PROGRESS`, `REENCRYPTION_IN_PROGRESS`, `ENCRYPTION_REQUIRED` |
| 413 | `PAYLOAD_TOO_LARGE` |
| 500 | `INTERNAL_ERROR`, `HOOK_ERROR`, `WRITER_WORKER_TIMEOUT` |
| 501 | `BULK_LOAD_UNSUPPORTED`, `SYNC_UNSUPPORTED`, `BACKUP_UNSUPPORTED`, `ENCRYPTION_UNSUPPORTED` |
| 502 | `BACKUP_DESTINATION_ERROR` |
| 503 | `DATABASE_CLOSED`, `SHUTDOWN`, `READ_CONCERN_ERROR`, `COORDINATOR_UNAVAILABLE`, `AUTHORITY_LOST`, `NO_SAFE_PRIMARY`, `NODE_NOT_IN_SYNC`, `NODE_DRAINING`, `UNSAFE_RECOVERY_REQUIRED`, `WRITE_OVERLOADED`, `ENCRYPTION_KEY_UNAVAILABLE` |

A code not listed defaults to 500, and an error carrying an explicit status uses it, which is how `authenticate` rejects with a status of its own. A `WRITE_OVERLOADED` response carries a `Retry-After` header in seconds, because the rejection is definite load shedding. `WRITER_WORKER_TIMEOUT` maps to 500 because its outcome is indeterminate. A coordinator-mode server that is not the current primary either forwards the write or rejects with `STALE_PRIMARY`, including the known primary endpoint as structured context when it has one.

A request body over `maxBodyBytes` is rejected with `413 PAYLOAD_TOO_LARGE` before it is fully buffered; an empty body fails with `400 EMPTY_BODY` and invalid JSON with `400 INVALID_JSON`.

---

## WebSocket Protocol (Normative)

A WebSocket connects at `/db/{id}` and supports queries, writes, and CDC subscriptions.

### Subprotocol Negotiation

The server supports one subprotocol, the plain identifier `sirannon.v1`. It must select that identifier when an upgrade offers it, refuse an upgrade whose offer omits it with `400 UNSUPPORTED_SUBPROTOCOL`, and select none when an upgrade offers no subprotocol. A client that configures subprotocols must offer `sirannon.v1` ahead of them. The `authenticate` hook receives the whole offer as the `sec-websocket-protocol` header.

### Client Messages

```text
{ type: 'subscribe',   id, table, tables?, filter?, sinceSeq?, epoch?, deviceId?, schemaVersion?, stagedStream? }
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
{ type: 'subscribed',   id, seq?, epoch?, resync?, rows?, maxUnacknowledgedChanges? }
{ type: 'unsubscribed', id }
{ type: 'change',       id, event: { type, table, row, oldRow?, seq, timestamp, hlc?, origin?, rowId?, txId?, txEnd? } }
{ type: 'changes',      id, events: List<{ type, table, row, oldRow?, seq, timestamp, hlc?, origin?, rowId?, txId?, txEnd? }> }
{ type: 'live',         id, ops?, rows?, revalidating? }
{ type: 'result',       id, data }     -- data is a query, execute, transaction, batch, load, or ack response
{ type: 'error',        id, error: { code, message } }
```

Every client message carries a string `id` the server echoes to correlate the reply; for a subscription the `id` is the subscription identifier. `sinceSeq`, `seq`, and `ack.seq` are decimal strings so sequence numbers beyond the safe integer range survive JSON. Change-event `row` and `oldRow` follow the value encoding, and `rowId` identifies the changed row. `hlc`, `origin`, and `txId` carry the change's timestamp, origin node, and transaction when it is stamped, and `txEnd` is true on the last change of a transaction (see [Transaction Boundaries](#transaction-boundaries)). A `changes` message carries several events in ascending `seq` order, each holding the fields of a `change` event; the server sends it only on a subscription carrying `stagedStream: true`. The `deviceId`, `schemaVersion`, `stagedStream`, and `ack` fields drive device sync, and `subscribed` carries `maxUnacknowledgedChanges` on a subscription presenting a `deviceId` (see [08-device-sync.md](08-device-sync.md)).

A message is rejected with `INVALID_JSON` when it is not JSON, `INVALID_MESSAGE` when it is not an object or lacks a string `type` or `id`, and `UNKNOWN_TYPE` for an unrecognised type. A subscription needs a string `table`, or a `tables` array of 1 to 500 table names in place of it; `tables` and `stagedStream` each require a `deviceId`, and `stagedStream` must be a boolean. A duplicate `id` fails with `DUPLICATE_SUBSCRIPTION`, a read-only database with `READ_ONLY`, and an in-memory database with `CDC_UNSUPPORTED`.

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

An inbound message over `maxBodyBytes` is rejected with `PAYLOAD_TOO_LARGE`. The server bounds each connection's outbound buffer by `maxWebSocketBackpressureBytes`; when a send would push the buffer past the bound, the server must close the connection with close code 4290 rather than drop a frame, so that the client detects the loss. A client that receives 4290 should reconnect and resume through subscription resumption. On a subscription presenting a `deviceId`, the server pauses delivery while the outbound buffer holds data and resumes from the change log once it drains, so that it delivers a transaction larger than the buffer across several sends and keeps the buffer under the bound. The server also closes with 1013 while shutting down, 1008 when the database is not found, closed, or the target resolves to none, and 4401 or 4403 when the `authenticate` hook refuses the upgrade. A client must not reconnect after 4401, 4403, or any code in the 4000-4099 range. The recommended idle timeout is 120 seconds with automatic ping/pong. The server paces a subscription presenting a `deviceId` by acknowledgements: it holds delivery once the highest sequence sent runs more than `maxUnacknowledgedChanges` ahead of that device's acknowledged cursor, and resumes on the next `ack`.

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
    "syncState": "ready", "healthReason": "in-sync",
    "readAvailability": "available", "writeAvailability": "available"
  }
}
```

`localSeq` and `primaryTerm` are stringified. `healthReason` is `NodeHealth.reason`, and `readAvailability` and `writeAvailability` are `available` when `NodeHealth` holds `canRead` and `canWrite` true. The readiness `status` is `NodeHealth.state`, with `healthy` reported as `ok`; a closed database reports `degraded` in place of `ok`.

---

## CORS

When CORS is enabled the server answers preflight `OPTIONS` requests with `204 No Content` and the allow headers, and attaches `Access-Control-Allow-Origin` to responses. Defaults are origin `*`, methods `GET, POST, OPTIONS`, headers `Content-Type, Authorization`, and `Access-Control-Max-Age: 86400`. A string origin is echoed; a list origin is echoed only when the request origin is listed. When the resolved origin is not `*`, the response includes `Vary: Origin`.
