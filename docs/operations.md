# Registered operations

A Sirannon server rejects SQL from the network until you set `acceptSql: true`. In its place, you register each statement that the server may execute under a name, and a caller sends that name with its arguments. The server holds every statement, so a caller can reach only the tables and columns that you registered.

## Register the operations

The registry is server-side code, keyed by database identifier. A read returns one statement, a write returns one statement or several, and the server executes every statement of a write in one transaction.

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

`args` names every argument that a caller may supply. A request that supplies an argument that you didn't declare fails with `ARGUMENT_NOT_ALLOWED`, and a request that leaves out a declared argument fails with `MISSING_ARGUMENT`. `columns` names the columns that a read returns, and code generation turns that list into a row type.

## Fill an argument from the caller's identity

`fromIdentity` maps an argument to a field of the identity that your `authenticate` hook returned, and the server fills that argument itself. A request that supplies such an argument fails with `ARGUMENT_NOT_ALLOWED`, so a caller can't overwrite the value that the server filled in.

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

TypeScript checks each `fromIdentity` value against the fields of your identity type, so a wrong field name fails to compile. A request with no identity, or with an identity that lacks that field, fails with `IDENTITY_REQUIRED`.

## Call an operation

Both client transports send named calls. Pass an `OperationRef` to `query` and `execute`; a plain string still means SQL, which the server rejects unless you turned SQL on.

```ts
import { operationRef } from '@delali/sirannon-db'

const ordersByStatus = operationRef<{ status: string }, { id: number; total: number; status: string }>('ordersByStatus')
const placeOrder = operationRef<{ total: number }>('placeOrder')

const orders = await db.query(ordersByStatus, { status: 'pending' })
const results = await db.execute(placeOrder, { total: 4999 })
```

A registered write returns one result per statement, so `execute` gives you an array, where the SQL form gives you a single result.

Over HTTP, the same calls go to two routes, and `{name}` is URL-encoded:

```text
POST /db/{id}/query/{name}    { args?, readConcern? }   -> { rows }
POST /db/{id}/execute/{name}  { args?, writeConcern? }  -> { results }
```

Over WebSocket, a `query` or an `execute` message that includes `name` and `args` calls the registered operation. The server resolves `fromIdentity` against the identity that your `authenticate` hook returned for the upgrade request.

## Announce what the server serves

`GET /capabilities` lists what a server supports and includes the registry digest. A server with operations, SQL, and device sync turned on responds along these lines, where the `sync.*` tokens continue through the device-sync set:

```json
{ "capabilities": ["query.named", "query.sql", "sync.push", "sync.ack"], "registry": { "digest": "9f2c..." } }
```

The digest is a hash over every registered database identifier, operation kind, operation name, declared argument name, and identity-filled argument name. It changes when you add, remove, or rename an operation, or change its arguments, which is how a client detects a rolling deploy. A live query sends the digest when it subscribes, and a server with a different digest rejects the subscription with `REGISTRY_MISMATCH`. The hash covers no statement text, no `columns` list, and no identity field that `fromIdentity` maps an argument to, so a changed row shape leaves the digest as it was; regenerate the client types when you change what a read returns.

`query.sql` tells a client that this server accepts statements. The client fetches `/capabilities` once, caches the response, and fails a statement with `SQL_NOT_ACCEPTED` before sending it when the token is absent. The server rejects the statement independently as well, because a hand-written client can skip that check.

## Turn SQL back on

Set `acceptSql: true` when you want the five statement routes and their WebSocket messages:

```ts
const server = createServer(sirannon, { port: 9876, acceptSql: true })
```

That server executes any statement that a caller sends, so authenticate every request and read the [security notes](../packages/ts/README.md#security) first. Registered operations stay available either way, and `acceptSql` has no effect on them.

## Generate typed references

The `sirannon-codegen` binary imports the registry that your server is built from and writes the references that your client uses to call it. The types then come from the definitions that the server executes, and your continuous integration needs no server to be up.

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --out ./src/generated/operations.ts
```

The generator imports the registry module, so when that module is not JavaScript, start the generator under a loader for your source format. It uses an export named `operations` or a default export; pass `--export <name>` for any other name, and `--manifest <file>` to write the manifest as JSON alongside the types.

```ts
import { app } from './generated/operations'

const orders = await db.query(app.reads.ordersByStatus, { status: 'pending' })
await db.execute(app.writes.placeOrder, { total: 4999 })
```

The generated file also exports `registryDigest`, which is the registry's digest at the time that you generated the file.

Each read gets the row type built from its `columns`. A read that declares no `columns` gets its row type from the statement text, but only when it declares no arguments and fills none from identity, because an argument can change which statement the read returns. Every other read leaves the row shape open.

## Errors

| Code | When |
| --- | --- |
| `UNKNOWN_QUERY` | No operation of that name is registered for the database. |
| `MISSING_ARGUMENT` | The request left out a declared argument. |
| `ARGUMENT_NOT_ALLOWED` | The caller supplied an undeclared argument, or one that the server fills from identity. |
| `IDENTITY_REQUIRED` | An operation fills an argument from identity, and the request's identity lacks that field. |
| `REGISTRY_MISMATCH` | A live query sent a digest that differs from this server's digest. |
| `SQL_NOT_ACCEPTED` | The server accepts no SQL over the network. |

The normative definition is in [`packages/spec/05-server.md`](../packages/spec/05-server.md#registered-operations). A remote [live query](live-queries.md) reads through a registered read.
