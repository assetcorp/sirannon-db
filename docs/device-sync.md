# Device sync

Device sync keeps an end-user device's local database and a server database in step, offline-first and both ways. A device syncs the whole database and holds no primary authority. The [device sync specification](../packages/spec/08-device-sync.md) defines the wire protocol.

The routes are built into the server, so watch the tables you want to sync and start the server as usual:

```ts
const db = await sirannon.open('app', './data/app.db')
await db.watch('tasks')

await createServer(sirannon, { port: 9876 }).listen()
```

On the device, drive the loop with a `SyncController`:

```ts
import { SyncController } from '@delali/sirannon-db/client'

const sync = new SyncController(db, {
  url: 'https://api.example.com',
  databaseId: 'app',
  tables: ['tasks'],
  onChange: event => refreshView(event.table),
  onResyncRequired: () => setEditorEnabled(false),
  onSnapshotProgress: progress => showProgress(progress),
  onSnapshotComplete: outcome => {
    setEditorEnabled(outcome.databaseUsable)
    if (!outcome.ok) reportCopyFailure(outcome.error, outcome.retrying)
  },
})

await sync.start()
const status = await sync.status()
```

- `start()` checks capabilities, reconciles the migration handshake, opens the live pull, and starts the push loop. A server that announces no `sync.stream-apply` is refused with `SYNC_UNSUPPORTED`. `pause()` keeps the cursors, `resume()` restarts the loops, and `stop()` ends them.
- The controller stages every pulled change in `_sirannon_staged_changes` before it applies anything, then applies each complete server transaction and the pull cursor in one local transaction. A device that stops part-way keeps its staged tail, applies each complete transaction on the next open, and resumes the subscription from what it holds. `onChange` fires after the commit, including for a change staged before a restart.
- Conflicts run through `resolver`, which defaults to last-write-wins on the HLC and accepts a delete whatever the timestamps say.
- A device acknowledges a sequence only once it has committed it, staged or applied, and the server holds delivery to a device running more than `maxUnacknowledgedChanges` past its acknowledgement. The server reports that window on `subscribed`, and the device acknowledges immediately once it holds more than half of it.
- Because the controller stages before it applies, it declares `stagedStream: true` to a server announcing `sync.staged-stream`. That server packs several changes into each `changes` frame and measures the delivery window per change rather than per transaction, so a transaction larger than the window still moves. A server announcing no `sync.staged-stream` receives no such declaration and delivers one change per frame, so an older server still syncs.
- A fresh device, or one too far behind to resume, replaces its whole database from a server snapshot. Local reads and writes fail with `SNAPSHOT_IN_PROGRESS` while that runs, and a failure after the wipe begins keeps them failing until a later copy succeeds.
- `onResyncRequired` opens that window and `onSnapshotComplete` closes it, for a copy the controller downloads on its own as well as one you request with `downloadSnapshot()`. Bind your editor to `outcome.databaseUsable`, which is `true` once the copy succeeds and after a failure that left the database intact. On a failure, `outcome.error` carries the code and message, and `outcome.retrying` is `true` when the controller has scheduled another attempt and `false` when you own the next one.
- Schema changes arrive through the migration handshake, never the change feed. The server withholds rows a migration wrote and refuses a stale device with `MIGRATION_REQUIRED`, and the controller then fetches, verifies, and applies the missing migrations. Share one migration set across your server, web, and mobile builds.
- A device idle past the retention window, 30 days by default, is evicted and resyncs from a snapshot.

The `SyncControllerOptions` table is in the [configuration reference](configuration.md).
