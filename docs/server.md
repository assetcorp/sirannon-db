# Server

`@delali/sirannon-db/server` exposes a `Sirannon` registry over HTTP and WebSocket, powered by uWebSockets.js.

Install uWebSockets.js alongside the package before you import the server. The npm registry has no package named `uWebSockets.js`, so install the tagged GitHub release v20.69.0, which is the version in Sirannon's own development dependencies:

```bash
pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"
```

When uWebSockets.js is absent, Node.js fails `import '@delali/sirannon-db/server'` with `ERR_MODULE_NOT_FOUND`.

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, { port: 9876, operations })
await server.listen()
```

## What the server accepts

A server refuses SQL from the network by default. Register the statements that callers may invoke by name, following the [registered operations guide](operations.md). Set `acceptSql: true` to open the five statement routes and their WebSocket messages, and authenticate every request when you do.

`GET /capabilities` returns the capability tokens of this server. The list holds `query.named` and the registry digest once you configure operations, `query.sql` once you turn SQL on, and the device-sync tokens. A client reads that list before it sends a statement, and it fails the call with `SQL_NOT_ACCEPTED` when `query.sql` is absent.

## Authentication

The server calls the `authenticate` hook before every database route and every WebSocket upgrade. Return the caller's identity so that the server can fill each `fromIdentity` argument of a registered operation, and throw a `RequestDeniedError` to refuse the request with a status of your own. The server answers the health and capability endpoints without calling the hook.

```ts
import { RequestDeniedError } from '@delali/sirannon-db'

const server = createServer<Identity>(sirannon, {
  port: 9876,
  cors: { origin: ['https://app.example.com'] },
  operations,
  authenticate: ctx => {
    const identity = verifyBearerToken(ctx.headers.authorization)
    if (!identity) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Invalid or missing token')
    return identity
  },
})
```

Throw to refuse a request, because the server fails a request with `500 HOOK_ERROR` when the hook returns a `{ status, code, message }` object.

A Node client attaches an `Authorization` header to the upgrade, so the hook finds the same `ctx.headers.authorization` on an HTTP route and on a WebSocket upgrade. A browser attaches no header to `new WebSocket(...)`, so accept a short-lived value in `Sec-WebSocket-Protocol` there, which the client sends through `webSocketProtocols`. Read `method` and `path` to spot the upgrade, and check the `Origin` header against an allowlist in the same hook.

The server supports one subprotocol, the plain identifier `sirannon.v1`, and selects it whenever the client offers it. When a client offers subprotocols without it, the upgrade fails with `400 UNSUPPORTED_SUBPROTOCOL`, while a client that offers none at all connects. Because the server selects the plain identifier, a credential stays out of the handshake response, while a browser still receives the selected protocol that it requires.

A WebSocket client has no access to the status of a refused handshake. So when your hook throws with status 401 or 403, the server completes the handshake and closes the connection at once with code 4401 or 4403, with your error code and message as the close reason. The server answers a refusal with any other status as an ordinary HTTP response with that status.

`GET /db/{id}/cluster` returns the address of every node in the group, so the server answers it only when `authorizeClusterStatus` accepts the request.

## Write shapes

The server accepts writes in three shapes on both transports: `transaction` for several different statements that must succeed or fail together, `batch` for one statement over many parameter sets, and `load` for a large import that trades durability for speed. A registered write is a fourth shape, whose statements the server also executes in one transaction.

The server executes each transaction, batch, and load in one transaction and replies once. The server holds the write lock only within one message, so every transaction must begin and commit inside a single message.

## Writer worker

Turn on `writerWorker` to move writes, checkpoints, loads, migrations, and backups onto a worker thread, so that a disk flush leaves the serving thread free. A write still returns only after its flush completes, so full durability stays intact:

```ts
const db = await sirannon.open('app', './data/app.db', {
  synchronous: 'full',
  writerWorker: { maxPendingWrites: 1024, writeTimeoutMs: 30_000, maxRestarts: 5 },
})
```

## HTTP routes

Clients URL-encode `{id}` and `{name}`. The server serves the five statement routes only once you set `acceptSql: true`, while it registers the other routes in every configuration.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/db/{id}/query` | Executes a SELECT and returns `{ rows }` |
| `POST` | `/db/{id}/query/{name}` | Executes a registered read and returns `{ rows }` |
| `POST` | `/db/{id}/execute` | Executes a mutation and returns `{ changes, lastInsertRowId }` |
| `POST` | `/db/{id}/execute/{name}` | Executes a registered write and returns `{ results }` |
| `POST` | `/db/{id}/transaction` | Executes many statements atomically and returns `{ results }` |
| `POST` | `/db/{id}/batch` | Applies one statement over many parameter sets and returns `{ results }` |
| `POST` | `/db/{id}/load` | Bulk-loads rows with relaxed durability and returns `{ rowsLoaded, changes }` |
| `POST` | `/db/{id}/changes` | Applies a device-sync change batch |
| `POST` | `/db/{id}/migrations` | Lists the migrations that a database has applied |
| `POST` | `/db/{id}/snapshot` | Opens a snapshot and returns its manifest |
| `POST` | `/db/{id}/snapshot/page` | Reads one page of a snapshot |
| `POST` | `/db/{id}/backup` | Starts one turn of the checkpoint cycle and returns `202` |
| `GET` | `/db/{id}/backup` | Returns what the cycle is doing and what its recent turns produced |
| `GET` | `/db/{id}/backup/chain` | Lists every chain at the backup destination, newest first |
| `POST` | `/db/{id}/backup/verify` | Reads one stored backup back and checks it |
| `POST` | `/db/{id}/backup/safe-to-delete` | Lists the records that no restore still needs |
| `POST` | `/db/{id}/backup/restore` | Rebuilds the database from a moment and returns `202` |
| `GET` | `/db/{id}/backup/restore` | Returns the outcome of that restore |
| `GET` | `/db/{id}/cluster` | Returns the role, replication group, current primary, primary term, read endpoints, and health |
| `GET` | `/capabilities` | Returns the announced capabilities and the registry digest |
| `GET` | `/health` | Answers a liveness check |
| `GET` | `/health/ready` | Answers a readiness check with per-database status |

Read the [device sync guide](device-sync.md) for the four device routes. A read body can hold `readConcern` and a write body can hold `writeConcern`, and you'll find both in the [replication guide](replication.md#read-concern).

## Backup routes

The server calls your `authenticate` hook before each backup route, as it does before every other `/db/{id}` route. Read the [backups guide](backups.md) for the checkpoint cycle behind these routes.

Reserve them for an operator credential. Your hook receives `ctx.path` and `ctx.method` on every request, so one hook can admit your application on the data routes and refuse it on these:

```ts
authenticate: ctx => {
  const identity = verifyToken(ctx.headers.authorization)
  if (ctx.path.startsWith(`/db/${ctx.databaseId}/backup`) && !identity.operator) {
    throw new RequestDeniedError(403, 'HOOK_DENIED', 'Only an operator may reach the backups')
  }
  return identity
}
```

Without a check of that shape, every identity that your hook accepts can call all seven backup routes, including the one that replaces the database when `acceptBackupRestore` is on.

The server answers a triggered backup with `202 Accepted` straight away, because a full copy of a large database can outlast the timeout of a proxy between you and the server. Read the outcome from the matching `GET`:

```bash
curl -XPOST -H "$AUTH" https://db.example.com/db/orders/backup
curl -H "$AUTH" https://db.example.com/db/orders/backup
```

That progress route answers with `running`, the `chainId` that the cycle is extending, the `progress` of the turn under way, and the `lastRun`, `lastSkip`, and `lastError` that the cycle recorded. When you trigger a second backup during a turn, the server queues one turn behind it. The server folds every later trigger into that queued turn, so at most one turn waits at a time.

For a database that you opened without the `backups` option, the server answers `501 BACKUP_UNSUPPORTED` on every one of these routes except `GET /db/{id}/backup/restore`, which reads the server's record of restores and leaves the database alone. However, the server checks `acceptBackupRestore` on `POST /db/{id}/backup/restore` before it looks the database up, so with that option off it answers `403 BACKUP_RESTORE_NOT_ACCEPTED` even for a database without `backups`. `POST /db/{id}/backup/verify` takes `{ name }`, where `name` comes from an entry in the chain route's response. `POST /db/{id}/backup/safe-to-delete` takes an optional `{ restorableFrom }`.

### Restoring over the network

`POST /db/{id}/backup/restore` stays shut until you set `acceptBackupRestore: true`. A restore replaces the database that serves your traffic, so the route is closed in every default configuration.

```ts
const server = createServer(sirannon, {
  authenticate: identifyOperator,
  acceptBackupRestore: true,
})
```

You must supply that hook here. `createServer` throws `INVALID_BACKUP_RESTORE` when you set `acceptBackupRestore: true` without an `authenticate` hook, because only that hook identifies the caller of a route that can destroy a database.

Sirannon rebuilds the database at its current path from the moment that you send:

```bash
curl -XPOST -H "$AUTH" -d '{"moment":1755500000000}' https://db.example.com/db/orders/backup/restore
curl -H "$AUTH" https://db.example.com/db/orders/backup/restore
```

The server closes the database first, which captures its log one last time. It then discards the chain that the old file was extending and rebuilds the file from that database's own backups. Finally, it opens the database again under the same identifier with the settings that it had. While the rebuild proceeds, every route answers `404 DATABASE_NOT_FOUND` for that identifier, so the status route reads the server's own record of the restore. After the reopen, the first turn of the cycle copies the whole database and starts a fresh chain, because the rebuilt file's log shares no history with the old chain. Sirannon takes that full copy to keep the order safe, because it discards the chain before it replaces the file. A process that dies part-way through a restore therefore can't resume capturing onto a chain that the restore replaced.

When you request a second restore of the same database during a restore, the server answers `409 BACKUP_RESTORE_IN_PROGRESS`. When a rebuild fails, the server still opens the database again, and the status route returns the error code of that failure. When the close fails, the server leaves nothing open under that identifier, because a second runtime over a file that the old connections may still be using would put two writers on one database. When the reopen fails after a successful rebuild, the status route reports `done` with the report and a separate `reopenError`, because Sirannon replaced the data and you need only restart the process.

## WebSocket messages

Connect to `ws://host:port/db/{id}`. Every message has a `type` and an `id` that the client chooses, and the server echoes that `id` in its reply. Both sides encode sequence numbers as decimal strings, so JSON keeps a value beyond the safe integer range exact.

| Inbound `type` | Fields | Reply |
| --- | --- | --- |
| `query` | `sql`, `params?`, `readConcern?` | `{ type: 'result', data: { rows } }` |
| `query` | `name`, `args?`, `readConcern?` | `{ type: 'result', data: { rows } }` |
| `execute` | `sql`, `params?` | `{ type: 'result', data: { changes, lastInsertRowId } }` |
| `execute` | `name`, `args?`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `transaction` | `statements`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `batch` | `sql`, `paramsBatch`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `load` | `sql`, `paramsBatch`, `durability?`, `checkpoint?` | `{ type: 'result', data: { rowsLoaded, changes } }` |
| `subscribe` | `table`, `tables?`, `filter?`, `sinceSeq?`, `epoch?`, `deviceId?`, `schemaVersion?`, `stagedStream?` | `{ type: 'subscribed', seq?, epoch?, resync?, maxUnacknowledgedChanges? }`, then change events |
| `subscribe` | `name`, `args?`, `registryDigest?` | `{ type: 'subscribed', rows }`, then `live` messages |
| `unsubscribe` | - | `{ type: 'unsubscribed' }` |
| `ack` | `deviceId`, `seq` | `{ type: 'result', data: { acked, seq } }` |

A `query` or `execute` message with `name` invokes the registered operation of that name and holds no SQL, so the server accepts it whether or not you set `acceptSql`.

| Outbound `type` | Contents |
| --- | --- |
| `change` | One change event: `type`, `table`, `row`, `oldRow?`, `seq`, `timestamp`, `hlc?`, `origin?`, `rowId?`, `txId?`, `txEnd?` |
| `changes` | Several change events in ascending `seq` order, sent only to a subscription that asked for `stagedStream` |
| `live` | `ops`, `rows`, or `revalidating` for a live query |
| `result` | The reply to a query, execute, transaction, batch, load, or ack |
| `error` | `{ code, message }` |

The server marks the last change of each transaction with `txEnd` on every subscription, so a consumer can apply a whole transaction at once and show only states that the database held. The server treats a subscription with the `name` of a registered read as a [live query](live-queries.md), and a subscription with a `deviceId` as part of [device sync](device-sync.md).

Set `sinceSeq` to the highest sequence that the client processed to resume a subscription from there, and set `epoch` to the sequence space of that cursor. The server replays every retained change above that sequence, then sets `resync: true` when the cursor is below the retained history or its epoch differs from the server's.

When a send would push a connection's outbound buffer past `maxWebSocketBackpressureBytes`, the server closes that connection with code 4290, so the client reconnects and resumes from its cursor with no frame lost. The server closes a connection with 1013 while shutting down, with 1008 when the database is absent or closed, and with 4401 or 4403 when the `authenticate` hook refuses the upgrade. A client leaves a connection closed after 4401 or 4403, because every later try with the same credential would fail too.

## Value encoding

Both transports round-trip every SQLite value through JSON. The server and client encode a blob as `{ "__sirannon_blob": "<uppercase hex>" }` and an integer beyond the safe range as `{ "__sirannon_int": "<decimal string>" }`. The client SDK encodes and decodes these for you, so your application handles `BigInt` and `Uint8Array` values with no extra code. The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md).

The `ServerOptions` and `DatabaseOptions` tables are in the [configuration reference](configuration.md).
