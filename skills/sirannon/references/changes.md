# Change subscriptions, live queries, and hooks

## Reacting to row changes

```ts
await db.watch('orders')
const subscription = db.on('orders').subscribe(event => handle(event), { onError: error => log(error) })
```

- Execute `db.watch(table)` before the subscription and before the writes that it has to see.
- Sirannon never awaits the callback. Chain the work onto one promise when each change has to finish before the next starts.
- A delete arrives with `row` set to `{}` and the old row in `oldRow`. With `.filter(...)`, a row that joins the filtered set arrives as an insert, and one that leaves it arrives as a delete.
- `onError` and `subscribe<T>` need 0.3.1 or newer.

## Keeping a result current

Use `db.live(sql, params)` when the app needs the current rows, and a subscription when it needs each event. A live query watches its own tables:

```ts
const live = await db.live<Order>('SELECT id, total FROM orders WHERE status = ?', ['pending'])
live.subscribe(() => {
  const state = live.getState()
  if (state.status === 'ready') render(state.rows)
})
```

Call `await live.close()` when the view goes away.

In React, install `react` and use `useLiveQuery(db, sql, params)` from `@delali/sirannon-db/react`. Through the client SDK, pass a registered read and its arguments in place of the SQL.

## Hooks and metrics

- Pass hooks for every database as `new Sirannon({ hooks })`, or add them to one database with `db.onBeforeQuery` and `db.onAfterQuery`.
- Keep `onBeforeQuery` synchronous, and throw `new HookDeniedError('onBeforeQuery', reason)` to refuse a statement. The server answers that refusal with status 403.
- Sirannon calls `onAfterQuery` without awaiting it, and it never calls that hook for a refused statement.
- Pass metrics callbacks as `new Sirannon({ metrics })`.
