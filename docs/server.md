# Server

`@delali/sirannon-db/server` exposes a `Sirannon` registry over HTTP and WebSocket, powered by uWebSockets.js.

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, { port: 9876 })
await server.listen()
```

The server runs SQL sent by clients, so read the [security section](../packages/ts/README.md#security) before you expose it.

## Write shapes

Writes come in three shapes on both transports: `transaction` for several different statements that must succeed or fail together, `batch` for one statement over many parameter sets, and `load` for a large import that trades durability for speed.

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

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/db/:id/query` | Execute a SELECT, returns `{ rows }` |
| `POST` | `/db/:id/execute` | Execute a mutation, returns `{ changes, lastInsertRowId }` |
| `POST` | `/db/:id/transaction` | Execute many statements atomically, returns `{ results }` |
| `POST` | `/db/:id/batch` | Apply one statement over many parameter sets, returns `{ results }` |
| `POST` | `/db/:id/load` | Bulk-load rows with relaxed durability, returns `{ rowsLoaded, changes }` |
| `GET` | `/db/:id/cluster` | Role, replication group, current primary, primary term, read endpoints, and health |
| `GET` | `/health` | Liveness check |
| `GET` | `/health/ready` | Readiness check with per-database status |

## WebSocket messages

Connect to `ws://host:port/db/:id`. Every message carries a `type` and a client-chosen `id`, and every reply echoes that `id`.

| Inbound `type` | Fields | Reply |
| --- | --- | --- |
| `query` | `sql`, `params?` | `{ type: 'result', data: { rows } }` |
| `execute` | `sql`, `params?` | `{ type: 'result', data: { changes, lastInsertRowId } }` |
| `transaction` | `statements`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `batch` | `sql`, `paramsBatch`, `writeConcern?` | `{ type: 'result', data: { results } }` |
| `load` | `sql`, `paramsBatch`, `durability?`, `checkpoint?` | `{ type: 'result', data: { rowsLoaded, changes } }` |
| `subscribe` | `table`, `tables?`, `filter?`, `sinceSeq?`, `epoch?` | `{ type: 'subscribed', seq?, epoch?, resync? }` then `change` events |
| `unsubscribe` | - | `{ type: 'unsubscribed' }` |

## Value encoding

Both transports round-trip every SQLite value through JSON. A blob crosses as `{ "__sirannon_blob": "<uppercase hex>" }` and an integer beyond the safe range as `{ "__sirannon_int": "<decimal string>" }`. The client SDK encodes and decodes these for you, so `BigInt` and `Uint8Array` values need no application code. The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md).

The `ServerOptions` and `DatabaseOptions` tables are in the [configuration reference](configuration.md).
