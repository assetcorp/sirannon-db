# Live queries

A live query is a query result that Sirannon keeps current from change events. Each change updates the rows the result already holds, and the statement runs a second time only in the three cases listed below. A subscription reports rows that changed; a live query reports the answer.

## Locally

`db.live` takes the statement, reads once, and watches the statement's table.

```ts
const orders = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

const stop = orders.subscribe(update => {
  if (update.kind === 'ops') applySplices(update.ops)
  else render(orders.getState())
})

stop()
await orders.close()
```

`getState()` returns `{ status: 'pending' }`, then `{ status: 'ready', rows, revalidating }`, or `{ status: 'error', error }`. Each update carries one of four kinds:

| Kind | Meaning |
| --- | --- |
| `ops` | Splices that produced the new rows, in order, one message per transaction |
| `rows` | A second read replaced the rows |
| `revalidating` | A second read is running and the held rows are the last complete answer |
| `error` | The query failed and `getState()` carries the error |

Each `ResultOp` is `{ op: 'insert', index, row }`, `{ op: 'update', index, row }`, or `{ op: 'delete', index }`. Apply them in order to hold the same rows the query holds. Code that only renders the result can read `getState()` and skip the operations.

Each live query owns one temporary probe table whose columns match the declared types and collations of the base table. Sirannon writes the row before and after every change into that table, then runs the statement's own `WHERE` clause and select list over those rows, so affinity, collation, and `ORDER BY` match a read of the base table. Closing the query drops that table.

### When the statement runs again

Sirannon reads a second time when a transaction carries more changes than the result has rows, when a `LIMIT` window loses a row the held rows can't replace, or when buffered changes exceed `maxTransactionChanges` or an internal byte bound. `revalidating` is true for the duration of that read, and the previous rows stay readable. `rereadJitterMs` bounds a random delay before it starts.

### What a live query maintains

A live query maintains the result of a single-table statement. `live` fails with `CDC_ERROR` for a join, an aggregate, `GROUP BY`, `HAVING`, `DISTINCT`, a compound `SELECT`, a common table expression, a `VALUES` clause, a window function, a subquery, or `LIMIT` without `ORDER BY`. A statement calling `random()`, `randomblob()`, `changes()`, `last_insert_rowid()`, `total_changes()`, or a clock function such as `datetime('now')` fails for the same reason, because its answer would change without a change event. An `ORDER BY` term bound positionally fails when the statement uses `?` parameters, because the sort term must name its column. `live` on a read-only database fails with `READ_ONLY`, since watching a table installs triggers.

## Over the network

A remote live query runs over a [registered read](operations.md), so the statement never crosses the network. The server holds the result and sends the operations that maintain it, and the client applies them in order.

```ts
import { operationRef } from '@delali/sirannon-db'

const ordersByStatus = operationRef<{ status: string }, { id: number; total: number }>('ordersByStatus')

const orders = await db.live(ordersByStatus, { status: 'pending' })
```

The rows arrive with the subscription reply, so no separate read is needed and no change falls between the two messages. The client echoes the registry digest when it subscribes; after `REGISTRY_MISMATCH` it re-reads `/capabilities` once and subscribes again, and fails the query when that attempt is refused too.

A live query needs the WebSocket transport and fails with `TRANSPORT_ERROR` over HTTP. While the connection is down the query holds its rows and reports `revalidating`; the transport subscribes again on reconnection and the server sends the rows afresh. The server holds the result, so a client resumes by subscribing again rather than from a cursor.

A remote live query carries no options. The server opens the query with its own defaults, so `rereadJitterMs` and `maxTransactionChanges` apply only to a local `db.live`. A subscription against an in-memory database fails with `CDC_UNSUPPORTED`.

## In React

`@delali/sirannon-db/react` wraps a live query in `useSyncExternalStore`, so a component re-renders when the result changes.

```tsx
import { useCommand, useLiveQuery } from '@delali/sirannon-db/react'
import { app } from './generated/operations'

function OrderList({ db }: { db: RemoteDatabase }) {
  const orders = useLiveQuery(db, app.reads.ordersByStatus, { status: 'pending' })
  const placeOrder = useCommand(db, app.writes.placeOrder)

  if (orders.status === 'pending') return <Spinner />
  if (orders.status === 'error') return <ErrorPanel error={orders.error} />

  return (
    <>
      <OrderTable rows={orders.rows} stale={orders.revalidating} />
      <NewOrderForm onSubmit={total => placeOrder({ total })} />
    </>
  )
}
```

`useLiveQuery` returns the same `LiveQueryState` the core API returns. Pass `enabled: false` to hold a query closed until you need it. `rereadJitterMs` and `maxTransactionChanges` reach the query only when the database is a local `Database`, because a remote subscription carries no options. `useCommand` returns a stable callback that runs a registered write.

The hooks compare arguments by value, so an inline object argument re-renders without reopening the query.

The `LiveQueryOptions` table is in the [configuration reference](configuration.md). The normative definition is in [`packages/spec/02-core.md`](../packages/spec/02-core.md#live-queries).
