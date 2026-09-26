# Change subscriptions, live queries, and hooks

## Change subscriptions

Sirannon records the changes to a table once you watch it, and delivers each recorded change to that table's subscribers:

```ts
await db.watch('orders')

const subscription = db
  .on('orders')
  .filter({ status: 'shipped' })
  .subscribe<{ id: number; status: string }>(
    event => console.log(event.type, event.row, event.oldRow, event.seq),
    { onError: error => console.error('order handler failed', error) },
  )

subscription.unsubscribe()
```

- Call `db.watch(table)` before you subscribe and before the writes that you want to receive. A subscription on a table that nothing watches receives no events and reports no error, and a watch captures no write made before it.
- `event.type` is `'insert'`, `'update'`, or `'delete'`. For a delete, `event.row` is an empty object and `event.oldRow` holds the previous row. `event.seq` is a `bigint`, and `event.timestamp` counts milliseconds since the Unix epoch.
- With a filter, an update that moves a row into the matching set arrives as an insert, and one that moves a row out arrives as a delete, so read `type` as the row joining or leaving the set.
- Sirannon never awaits the callback, so two calls to an asynchronous callback can overlap. Chain the work onto one promise when each change has to finish before the next one starts.
- `onError` receives a throw or a rejection from the callback, and the failure that stops the change-log poll after ten errors in a row. `onError` and the type parameter on `subscribe` need 0.3.1 or newer.

## Live queries

A live query holds the current result of a statement and applies each change to the rows that it already holds. It watches the tables that it reads by itself, so it needs no `db.watch` call:

```ts
const pending = await db.live<{ id: number; total: number }>(
  'SELECT id, total FROM orders WHERE status = ? ORDER BY id',
  ['pending'],
)

const stop = pending.subscribe(update => {
  const state = pending.getState()
  if (state.status === 'ready') render(state.rows)
})

stop()
await pending.close()
```

`getState()` returns `{ status: 'pending' }`, `{ status: 'ready', rows, revalidating }`, or `{ status: 'error', error }`. Each update has a `kind` of `'rows'`, `'ops'`, `'revalidating'`, or `'error'`, and an `'ops'` update lists the index of each changed row, with the new row for an insert or an update.

Neither a live query nor a change subscription keeps the Node.js process alive, so a script that awaits nothing else exits before the first update arrives. A server's listening socket keeps its process alive, so this matters in scripts and tests, where the wait needs a `setTimeout` deadline of its own.

Read `live-queries.md` in the versioned documentation for the statements that a live query can maintain.

## React

Install React, since `@delali/sirannon-db/react` imports it and fails to load without it:

```bash
pnpm add -E react
```

`useLiveQuery` takes a local `Database` with a SQL string and its parameters, or a `RemoteDatabase` from the client with a registered read and its arguments. It returns the same `LiveQueryState` that `getState()` returns:

```tsx
import { useLiveQuery } from '@delali/sirannon-db/react'

const state = useLiveQuery<{ id: number; total: number }>(db, 'SELECT id, total FROM orders ORDER BY id')
```

`useCommand(database, command)` returns a function that executes a write, for a registered write through the client or a SQL string on a local database.

## Hooks and metrics

Register hooks on the registry through `new Sirannon({ hooks })`, or on one database through `db.onBeforeQuery(hook)` and `db.onAfterQuery(hook)`. Each registration returns a function that removes the hook.

- Write `onBeforeQuery` as a synchronous function, and throw `new HookDeniedError('onBeforeQuery', reason)` from it to refuse the statement. The caller receives that error with code `HOOK_DENIED`, and the server answers it with status 403. An `async` before-hook returns a promise, which makes Sirannon throw for every statement that the hook sees.
- Sirannon calls `onAfterQuery` without awaiting it, and it discards anything that the hook throws. A hook that writes elsewhere has to chain those writes onto one promise, which the code awaits before it reads what the hook wrote.
- An `onAfterQuery` hook sees no statement that a before-hook refused, so record a refusal where the code catches the `HookDeniedError`.
- `metrics` in `SirannonOptions` takes `onQueryComplete`, `onConnectionOpen`, `onConnectionClose`, and `onCDCEvent` callbacks.

Read `change-data-capture.md` and `hooks-metrics-and-lifecycle.md` in the versioned documentation for the rest.
