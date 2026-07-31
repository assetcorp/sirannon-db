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

Browsers can't attach an `Authorization` header to `new WebSocket(...)`. Read `method` and `path` to spot the upgrade, check the `Origin` header against an allowlist, and accept a short-lived value in `Sec-WebSocket-Protocol` that the client passes through `webSocketProtocols`.

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
| `GET` | `/db/{id}/cluster` | Role, replication group, current primary, primary term, read endpoints, and health |
| `GET` | `/capabilities` | Announced capabilities and the registry digest |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check with per-database status |

The [device sync guide](device-sync.md) covers the four device routes. A read body carries `readConcern` and a write body carries `writeConcern`; the [replication guide](replication.md#read-concern) defines both.

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

When a send would push a connection's outbound buffer past `maxWebSocketBackpressureBytes`, the server closes that connection with code 4290 rather than dropping a frame, so the client reconnects and resumes from its cursor.

## Value encoding

Both transports round-trip every SQLite value through JSON. A blob crosses as `{ "__sirannon_blob": "<uppercase hex>" }` and an integer beyond the safe range as `{ "__sirannon_int": "<decimal string>" }`. The client SDK encodes and decodes these for you, so `BigInt` and `Uint8Array` values need no application code. The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md).

The `ServerOptions` and `DatabaseOptions` tables are in the [configuration reference](configuration.md).
