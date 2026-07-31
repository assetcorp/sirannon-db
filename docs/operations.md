# Registered operations

A Sirannon server accepts no SQL from the network until you set `acceptSql: true`. Instead, you register each statement the server may run under a name, and a caller sends that name with its arguments. The server holds every statement, so a caller reaches only the tables and columns you registered.

## Register the operations

The registry is server-side code, keyed by database identifier. A read returns one statement, a write returns one statement or several, and the server runs every statement of a write in one transaction.

```ts
import { createServer } from '@delali/sirannon-db/server'

const server = createServer(sirannon, {
  port: 9876,
  operations: {
    app: {
      reads: {
        ordersByStatus: {
          args: ['status'],
          columns: ['id', 'total', 'status'],
          statement: ({ status }) => ({
            sql: 'SELECT id, total, status FROM orders WHERE status = ? ORDER BY id',
            params: [status],
          }),
        },
      },
      writes: {
        placeOrder: {
          args: ['total'],
          statements: ({ total }) => [
            { sql: 'INSERT INTO orders (total, status) VALUES (?, ?)', params: [total, 'pending'] },
            { sql: 'UPDATE counters SET orders = orders + 1' },
          ],
        },
      },
    },
  },
})

await server.listen()
```

`args` names every argument a caller may supply. A caller that supplies an argument you didn't declare fails with `ARGUMENT_NOT_ALLOWED`, and one that leaves a declared argument out fails with `MISSING_ARGUMENT`. `columns` names what a read returns, and code generation turns that list into a row type.

## Fill an argument from the caller's identity

`fromIdentity` maps an argument to a field of the identity your `authenticate` hook returned, and the server fills that argument itself. A request that supplies such an argument fails with `ARGUMENT_NOT_ALLOWED`, so a caller can't overwrite the value the server chose.

```ts
interface Identity {
  userId: string
}

const server = createServer<Identity>(sirannon, {
  port: 9876,
  authenticate: ctx => verifyBearerToken(ctx.headers.authorization),
  operations: {
    app: {
      reads: {
        myOrders: {
          fromIdentity: { ownerId: 'userId' },
          columns: ['id', 'total'],
          statement: ({ ownerId }) => ({
            sql: 'SELECT id, total FROM orders WHERE owner_id = ?',
            params: [ownerId],
          }),
        },
      },
    },
  },
})
```

TypeScript checks each `fromIdentity` value against the fields of your identity type, so a wrong field name fails to compile. A request that carries no identity fails with `IDENTITY_REQUIRED`.

## Call an operation

Both client transports carry named calls. Pass an `OperationRef` to `query` and `execute`; a plain string still means SQL, which the server refuses unless you turned SQL on.

```ts
import { operationRef } from '@delali/sirannon-db'

const ordersByStatus = operationRef<{ status: string }, { id: number; total: number; status: string }>('ordersByStatus')
const placeOrder = operationRef<{ total: number }>('placeOrder')

const orders = await db.query(ordersByStatus, { status: 'pending' })
const results = await db.execute(placeOrder, { total: 4999 })
```

A registered write returns one result per statement, so `execute` gives you an array where the SQL form gives you a single result.

Over HTTP the same calls are two routes, and `{name}` is URL-encoded:

```text
POST /db/{id}/query/{name}    { args?, readConcern? }   -> { rows }
POST /db/{id}/execute/{name}  { args?, writeConcern? }  -> { results }
```

Over WebSocket, a `query` or an `execute` message carrying `name` and `args` runs the registered operation. The server resolves `fromIdentity` against the identity your `authenticate` hook returned for the upgrade request.

## Announce what the server serves

`GET /capabilities` lists what a server supports and carries the registry digest. A server running operations, statements, and device sync answers along these lines, where the `sync.*` tokens continue through the device-sync set:

```json
{ "capabilities": ["query.named", "query.sql", "sync.push", "sync.ack"], "registry": { "digest": "9f2c..." } }
```

The digest is a hash over every registered database identifier, operation kind, operation name, and argument name. It changes whenever the contract a client generates against changes, which is how a client notices a rolling deploy. A live query echoes the digest when it subscribes, and a server serving a different one refuses with `REGISTRY_MISMATCH`.

`query.sql` tells a client that this server accepts statements. The client reads `/capabilities` once, caches the answer, and fails a statement with `SQL_NOT_ACCEPTED` before it leaves the process when the token is absent. The server refuses on its own as well, because a hand-written client runs no such check.

## Turn SQL back on

Set `acceptSql: true` when you want the five statement routes and their WebSocket messages:

```ts
const server = createServer(sirannon, { port: 9876, acceptSql: true })
```

That server runs any statement a caller sends, so authenticate every request and read the [security notes](../packages/ts/README.md#security) first. Registered operations stay available either way, and `acceptSql` never governs them.

## Generate typed references

The `sirannon-codegen` binary reads the registry your server is built from and writes the references your client calls it through. The types then come from the definitions the server runs, and your continuous integration needs no running server.

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --out ./src/generated/operations.ts
```

The generator imports the registry module, so run it under a loader that reads your source format when that module is not JavaScript. It reads an export named `operations` or a default export; pass `--export <name>` for any other name, and `--manifest <file>` to write the manifest as JSON alongside the types.

```ts
import { app } from './generated/operations'

const orders = await db.query(app.reads.ordersByStatus, { status: 'pending' })
await db.execute(app.writes.placeOrder, { total: 4999 })
```

The generated file also exports `registryDigest`, the digest the registry carried when you generated it.

Each read carries the row type built from its `columns`. A read that declares none takes its row type from the statement, and only when it declares no arguments at all, because an argument chooses the statement. Every other read leaves the row shape open.

## Errors

| Code | When |
| --- | --- |
| `UNKNOWN_QUERY` | No operation of that name is registered for the database |
| `MISSING_ARGUMENT` | A declared argument was absent from the request |
| `ARGUMENT_NOT_ALLOWED` | The caller supplied an undeclared argument, or one the server fills from identity |
| `IDENTITY_REQUIRED` | An operation fills an argument from identity and the request carries none |
| `REGISTRY_MISMATCH` | A live query echoed a digest this server does not serve |
| `SQL_NOT_ACCEPTED` | The server accepts no SQL over the network |

The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md#registered-operations). [Live queries](live-queries.md) run over a registered read.
