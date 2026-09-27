# Live queries

A live query is a query result that Sirannon keeps current from change events. Sirannon applies each change to the rows that the result already holds, and it reads the statement a second time only in the cases listed below. A subscription reports the rows that changed, while a live query reports the whole current answer.

## Locally

`db.live` takes the statement, reads it once, and starts watching the statement's table.

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

`getState()` returns `{ status: 'pending' }`, then `{ status: 'ready', rows, revalidating }`, or `{ status: 'error', error }`. Each update has one of four kinds:

| Kind | Meaning |
| --- | --- |
| `ops` | The update lists the splices that produced the new rows, in order, with one message per transaction. |
| `rows` | A second read has replaced the rows. |
| `revalidating` | A second read is under way, and the rows that the query holds are the last complete answer. |
| `error` | The query failed, and `getState()` returns the error. |

Each `ResultOp` is `{ op: 'insert', index, row }`, `{ op: 'update', index, row }`, or `{ op: 'delete', index }`. When you apply the operations in order, your copy holds the same rows as the query. Code that only renders the result can call `getState()` and skip the operations.

Each live query has its own temporary probe table, whose columns match the declared types and collations of the base table. Sirannon writes the row as it stood before and after every change into that table, and then it evaluates the statement's own `WHERE` clause and select list over those rows, so affinity, collation, and `ORDER BY` behave as they do in a read of the base table. Closing the query drops that table.

### When the statement runs again

Sirannon reads the statement a second time in these cases:

- A transaction produces more changes to the result than the result has rows.
- A change leaves a `LIMIT` window short of a row that the held rows cannot supply.
- The statement has an `OFFSET`, and a change removes a held row, touches a matching row that the window does not hold, or adds a row that sorts at or before the first held row.
- The changes buffered for one transaction exceed `maxTransactionChanges`, which is 10,000 by default, or an internal limit of roughly 16 MiB.
- The change feed overflows its buffer while the query is starting.

`revalidating` is true for as long as that read lasts, and the previous rows stay readable. Before the read starts, Sirannon waits a random delay of up to `rereadJitterMs`, which is 25 ms by default.

### What a live query maintains

A live query maintains the result of a single-table statement. `live` fails with `CDC_ERROR` for a join, an aggregate, `GROUP BY`, `HAVING`, `DISTINCT`, a compound `SELECT`, a common table expression, a `VALUES` clause, a window function, a subquery, or `LIMIT` without `ORDER BY`. A statement that calls `random()`, `randomblob()`, `changes()`, `last_insert_rowid()`, `total_changes()`, or a clock function such as `datetime('now')` fails for the same reason, because its answer can change without any change event. When the statement binds `?` parameters, an `ORDER BY` term that holds a parameter fails, because Sirannon evaluates that term a second time to place a changed row and binds its parameter by name. `live` on a read-only database fails with `READ_ONLY`, because watching a table installs triggers.

## Over the network

A remote live query reads through a [registered read](operations.md), so the statement stays on the server. The server holds the result and sends the operations that keep it current, and the client applies them in order.

```ts
import { operationRef } from '@delali/sirannon-db'

const ordersByStatus = operationRef<{ status: string }, { id: number; total: number }>('ordersByStatus')

const orders = await db.live(ordersByStatus, { status: 'pending' })
```

The server sends the rows in its reply to the subscription, so the client needs no separate read and misses no change between the read and the subscription. The client sends the registry digest back when it subscribes. After a `REGISTRY_MISMATCH`, it reads `/capabilities` again once and subscribes a second time, and the query fails when the server rejects that second attempt as well.

A live query needs the WebSocket transport and fails with `TRANSPORT_ERROR` over HTTP. While the connection is down, the query keeps its rows and reports `revalidating`. When the connection returns, the transport subscribes again and the server sends the rows afresh, because the server holds the result and the client resumes by subscribing, not from a cursor.

A remote live query takes no options. The server opens the query with its own defaults, so `rereadJitterMs` and `maxTransactionChanges` apply only to a local `db.live`. A remote live query on an in-memory database fails with `CDC_UNSUPPORTED`.

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

`useLiveQuery` returns the same `LiveQueryState` as the core API. Pass `enabled: false` to keep a query closed until you need it. `rereadJitterMs` and `maxTransactionChanges` take effect only when the database is a local `Database`, because a remote subscription takes no options. `useCommand` returns a stable callback that calls a registered write.

The hooks compare arguments by value, so an inline object argument re-renders the component without reopening the query.

The `LiveQueryOptions` table is in the [configuration reference](configuration.md). The normative definition is in [`packages/spec/02-core.md`](../packages/spec/02-core.md#live-queries).
