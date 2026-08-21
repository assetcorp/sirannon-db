# Server

`@delali/sirannon-db/server` exposes a `Sirannon` registry over HTTP and WebSocket, powered by uWebSockets.js.

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, { port: 9876, operations })
await server.listen()
```

## What the server accepts

A server accepts no SQL from the network by default. Register the statements it may run and callers invoke them by name, as the [registered operations guide](operations.md) sets out. Set `acceptSql: true` to open the five statement routes and their WebSocket messages, and authenticate every request when you do.

`GET /capabilities` announces what this server supports: `query.named` and the registry digest once you configure operations, `query.sql` once you turn SQL on, and the device-sync tokens. A client reads that answer before it sends a statement and fails with `SQL_NOT_ACCEPTED` when `query.sql` is absent.

## Authentication

The `authenticate` hook runs before every database route and every WebSocket upgrade. Return the caller's identity, which registered operations then read through `fromIdentity`, and throw a `RequestDeniedError` to refuse the request with a status of your own. Health and capability endpoints skip the hook.

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

Every value the hook returns becomes the caller identity, so returning a `{ status, code, message }` object instead of throwing fails the request with `HOOK_ERROR`.

A Node client attaches an `Authorization` header to the upgrade, so `ctx.headers.authorization` reads the same on an HTTP route and on a WebSocket upgrade. A browser attaches no header to `new WebSocket(...)`, so accept a short-lived value in `Sec-WebSocket-Protocol` there, which the client sends through `webSocketProtocols`. Read `method` and `path` to spot the upgrade, and check the `Origin` header against an allowlist in the same hook.

The server supports one subprotocol, the plain identifier `sirannon.v1`, and selects it whenever the client offers it. An upgrade that offers subprotocols without it fails with `400 UNSUPPORTED_SUBPROTOCOL`, and an upgrade that offers none at all connects. Selecting the plain identifier keeps a credential out of the handshake response and gives a browser the selected protocol it requires.

No WebSocket client can read the status of a refused handshake, so when your hook throws with status 401 or 403 the server completes the handshake and closes the connection at once with code 4401 or 4403, carrying your error code and message as the close reason. A refusal with any other status keeps its HTTP status response.

`GET /db/{id}/cluster` reports the address of every node in the group, so it answers only a request that `authorizeClusterStatus` accepts.

## Write shapes

Writes come in three shapes on both transports: `transaction` for several different statements that must succeed or fail together, `batch` for one statement over many parameter sets, and `load` for a large import that trades durability for speed. A registered write is a fourth shape, and the server runs its statements in one transaction too.

Each transaction, batch, and load runs server-side in one transaction and replies once. The server never holds the write lock across a round-trip, so it accepts no interactive `BEGIN` ... `COMMIT` across messages.

## Writer worker

Turn on `writerWorker` to run writes, checkpoints, loads, migrations, and backups on a worker thread so a disk flush never blocks the serving thread. Full durability still holds, because a write returns only after its flush completes:

```ts
const db = await sirannon.open('app', './data/app.db', {
  synchronous: 'full',
  writerWorker: { maxPendingWrites: 1024, writeTimeoutMs: 30_000, maxRestarts: 5 },
})
```

## HTTP routes

`{id}` and `{name}` are URL-encoded. The five statement routes need `acceptSql: true`, and the server always serves the rest.

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/db/{id}/query` | Run a SELECT, returns `{ rows }` |
| `POST` | `/db/{id}/query/{name}` | Run a registered read, returns `{ rows }` |
| `POST` | `/db/{id}/execute` | Run a mutation, returns `{ changes, lastInsertRowId }` |
| `POST` | `/db/{id}/execute/{name}` | Run a registered write, returns `{ results }` |
| `POST` | `/db/{id}/transaction` | Run many statements atomically, returns `{ results }` |
| `POST` | `/db/{id}/batch` | Apply one statement over many parameter sets, returns `{ results }` |
| `POST` | `/db/{id}/load` | Bulk-load rows with relaxed durability, returns `{ rowsLoaded, changes }` |
| `POST` | `/db/{id}/changes` | Apply a device-sync change batch |
| `POST` | `/db/{id}/migrations` | List the migrations a database has applied |
| `POST` | `/db/{id}/snapshot` | Open a snapshot and return its manifest |
| `POST` | `/db/{id}/snapshot/page` | Read one page of a snapshot |
| `POST` | `/db/{id}/backup` | Run one turn of the checkpoint cycle, returns `202` |
| `GET` | `/db/{id}/backup` | What the cycle is doing and what its recent turns produced |
| `GET` | `/db/{id}/backup/chain` | Every chain at the backup destination, newest first |
| `POST` | `/db/{id}/backup/verify` | Read one stored backup back and check it |
| `POST` | `/db/{id}/backup/safe-to-delete` | The records no restore still needs |
| `POST` | `/db/{id}/backup/restore` | Rebuild the database from a moment, returns `202` |
| `GET` | `/db/{id}/backup/restore` | How that restore went |
| `GET` | `/db/{id}/cluster` | Role, replication group, current primary, primary term, read endpoints, and health |
| `GET` | `/capabilities` | Announced capabilities and the registry digest |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check with per-database status |

The [device sync guide](device-sync.md) covers the four device routes. A read body carries `readConcern` and a write body carries `writeConcern`; the [replication guide](replication.md#read-concern) defines both.

## Backup routes

The server serves these to an operator, and it runs your `authenticate` hook before each of them as before every other `/db/{id}` route. The [backups guide](backups.md) covers what the cycle behind them does.

Reserve them for an operator credential. Your hook receives `ctx.path` and `ctx.method` on every request, which is how a single hook admits your application on the data routes and refuses it here:

```ts
authenticate: ctx => {
  const identity = verifyToken(ctx.headers.authorization)
  if (ctx.path.startsWith(`/db/${ctx.databaseId}/backup`) && !identity.operator) {
    throw new RequestDeniedError(403, 'HOOK_DENIED', 'Only an operator may reach the backups')
  }
  return identity
}
```

Without a check of that shape, every identity your hook accepts may call all six routes, and with `acceptBackupRestore` on that includes the one that replaces the database.

The server answers a triggered backup with `202 Accepted` straight away and waits for no turn, since a full copy of a large database may continue past the deadline any proxy between you and the server allows. Read the outcome from the matching `GET`:

```bash
curl -XPOST -H "$AUTH" https://db.example.com/db/orders/backup
curl -H "$AUTH" https://db.example.com/db/orders/backup
```

That progress route answers with `running`, the `chainId` the cycle is extending, the `progress` of the turn under way, and the `lastRun`, `lastSkip`, and `lastError` it recorded. A second trigger sent while a turn is under way queues one behind it, and every trigger after that joins the queued turn, so at most one turn ever waits. A database you opened without the `backups` option answers `501 BACKUP_UNSUPPORTED` on all of these routes but `GET /db/{id}/backup/restore`, which reports on restores and reads no database. `POST /db/{id}/backup/verify` takes `{ name }`, which is the name any entry of the chain route states, and `POST /db/{id}/backup/safe-to-delete` takes an optional `{ restorableFrom }`.

### Restoring over the network

`POST /db/{id}/backup/restore` stays shut until you set `acceptBackupRestore: true`. A restore replaces the database that is serving your traffic, so every default configuration leaves that route closed.

```ts
const server = createServer(sirannon, {
  authenticate: identifyOperator,
  acceptBackupRestore: true,
})
```

That hook is required here. A server built with `acceptBackupRestore: true` and no `authenticate` refuses to start, since the hook is the only gate that names the caller of a route which destroys a database.

Name the moment you want back, and Sirannon rebuilds the database at the path it already occupies:

```bash
curl -XPOST -H "$AUTH" -d '{"moment":1755500000000}' https://db.example.com/db/orders/backup/restore
curl -H "$AUTH" https://db.example.com/db/orders/backup/restore
```

The server closes the database, and that close captures its log a final time. It then discards the chain the old file was extending, rebuilds the file from that database's own backups, and opens the database again under the same identifier with the settings it had. Every route answers `404 DATABASE_NOT_FOUND` for that identifier while the rebuild proceeds, which is why the status route reads the server's own record. The first turn of the cycle after the reopen copies the whole database and starts a fresh chain, since the rebuilt file's log continues none of the old one. You pay for that full copy in exchange for the safe order: Sirannon discards the chain before it replaces the file, so a process that dies part-way through a restore can never resume capturing onto a chain that restore has replaced.

A second restore of the same database while one is under way answers `409 BACKUP_RESTORE_IN_PROGRESS`. A rebuild that fails still opens the database again, and the status route states the code it stopped with. A close that fails leaves nothing open under that identifier, since a second runtime over a file the old connections may still be using would put two writers on one database. A reopen that fails after a successful rebuild reports `done` with the report and a separate `reopenError`, since Sirannon replaced the data either way and only the process needs restarting.

## WebSocket messages

Connect to `ws://host:port/db/{id}`. Every message carries a `type` and a client-chosen `id`, and every reply echoes that `id`. Sequence numbers cross as decimal strings, so a value beyond the safe integer range survives JSON.

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

A `query` or an `execute` message carrying `name` runs the registered operation of that name and carries no SQL, so `acceptSql` doesn't govern it.

| Outbound `type` | Carries |
| --- | --- |
| `change` | One change event: `type`, `table`, `row`, `oldRow?`, `seq`, `timestamp`, `hlc?`, `origin?`, `rowId?`, `txId?`, `txEnd?` |
| `changes` | Several change events in ascending `seq` order, sent only to a subscription that asked for `stagedStream` |
| `live` | `ops`, `rows`, or `revalidating` for a live query |
| `result` | The reply to a query, execute, transaction, batch, load, or ack |
| `error` | `{ code, message }` |

Every subscription marks the last change of each transaction with `txEnd`, so a consumer applies a whole transaction at once and never shows a state the database never held. A subscription naming a registered read is a [live query](live-queries.md), and one carrying a `deviceId` drives [device sync](device-sync.md).

`sinceSeq` resumes a subscription from the highest sequence the client processed, and `epoch` names the sequence space that cursor came from. The server replays every retained change above that sequence, then sets `resync: true` when the cursor fell below the retained history or arrived with a foreign epoch.

When a send would push a connection's outbound buffer past `maxWebSocketBackpressureBytes`, the server closes that connection with code 4290 rather than dropping a frame, so the client reconnects and resumes from its cursor. It closes with 1013 while shutting down, with 1008 when the database is absent or closed, and with 4401 or 4403 when the `authenticate` hook refuses the upgrade. A client leaves a connection closed after 4401 or 4403, because the same credential fails every later attempt.

## Value encoding

Both transports round-trip every SQLite value through JSON. A blob crosses as `{ "__sirannon_blob": "<uppercase hex>" }` and an integer beyond the safe range as `{ "__sirannon_int": "<decimal string>" }`. The client SDK encodes and decodes these for you, so `BigInt` and `Uint8Array` values need no application code. The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md).

The `ServerOptions` and `DatabaseOptions` tables are in the [configuration reference](configuration.md).
