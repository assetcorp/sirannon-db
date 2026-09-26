# Device sync

Device sync keeps the local database on an end-user device in step with a server database in both directions, so the device can go on working while it is offline. A device syncs the whole database and holds no primary authority. The [device sync specification](../packages/spec/08-device-sync.md) defines the network protocol.

A server keeps device sync off until you set `acceptDeviceSync: true`. With that option set, the server refuses to start unless you also give it an `authenticate` hook that identifies each caller. A push can write rows into any table of the database, so register an `onBeforePush` hook that checks that the caller owns the device. The hook below records the account that first pushes from each device and refuses every other account from then on:

```ts
import { HookDeniedError, Sirannon } from '@delali/sirannon-db'

const deviceOwners = new Map<string, string>()

const sirannon = new Sirannon({
  driver,
  hooks: {
    onBeforePush: ({ deviceId, identity }) => {
      const { userId } = identity as { userId: string }
      const owner = deviceOwners.get(deviceId) ?? userId
      if (owner !== userId) throw new HookDeniedError('beforePush', 'This device belongs to another account')
      deviceOwners.set(deviceId, owner)
    },
  },
})

const db = await sirannon.open('app', './data/app.db')
await db.watch('tasks')

await createServer(sirannon, {
  port: 9876,
  acceptDeviceSync: true,
  authenticate: ({ headers }) => verifySession(headers.authorization),
}).listen()
```

Store that owner record in your own database, because the map in this example empties whenever the process restarts. `onBeforeSubscribe` receives the same `deviceId` and identity when a device subscribes, so you can apply the same check on the pull side.

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

- `start()` checks capabilities, reconciles the migration handshake, opens the live pull, and starts the push loop. It fails with `SYNC_UNSUPPORTED` against a server that announces no `sync.stream-apply`, and with `DEVICE_SYNC_NOT_ACCEPTED` against a server that keeps device sync off. `pause()` keeps the cursors, `resume()` restarts the loops, and `stop()` ends them.
- The controller stages every pulled change in `_sirannon_staged_changes` before it applies anything. It then applies each complete server transaction and advances the pull cursor inside one local transaction. When a device stops part-way, it keeps its staged changes, so on the next open the controller applies each complete transaction and resumes the subscription from the last change that the device holds. `onChange` fires after the commit, including for a change that the device staged before a restart.
- The controller settles each conflict with `resolver`, which defaults to last-write-wins on the hybrid logical clock stamp and accepts a remote delete whatever the stamps on either side.
- The server refuses a push stamped more than five minutes ahead of its own clock with `DEVICE_CLOCK_AHEAD`, so a device with a fast clock cannot win every conflict. The controller keeps those writes and reports the code in `status().lastError`. It then retries them until the server clock comes within five minutes of their stamp.
- A device acknowledges a sequence only once it has committed it, whether staged or applied. The server holds back delivery to a device that is more than `maxUnacknowledgedChanges` past its acknowledgement, and it reports that window in the `subscribed` message. Once a device holds more than half of the window, the device acknowledges at once.
- Because the controller stages before it applies, it declares `stagedStream: true` to a server that announces `sync.staged-stream`. That server packs several changes into each `changes` frame and counts the delivery window in changes, so it can deliver a transaction that is larger than the whole window. The controller sends no such declaration to a server that announces no `sync.staged-stream`, so that older server goes on delivering one change per frame.
- A fresh device, or one too far behind to resume, replaces its whole database from a server snapshot. Local reads and writes fail with `SNAPSHOT_IN_PROGRESS` while the copy is in progress. When the copy fails after the wipe begins, they go on failing until a later copy succeeds.
- The controller calls `onResyncRequired` before that copy starts and `onSnapshotComplete` once it ends, both for a copy that it downloads on its own and for one that you request with `downloadSnapshot()`. Bind your editor to `outcome.databaseUsable`, which is `true` once the copy succeeds and after a failure that leaves the database intact. After a failure, `outcome.error` contains the code and the message. `outcome.retrying` is `true` when the controller schedules another attempt, and `false` when the next attempt is yours to make.
- The device receives schema changes through the migration handshake, never through the change feed. The server withholds the rows that a migration writes and refuses a stale device with `MIGRATION_REQUIRED`. The controller then fetches, verifies, and applies the missing migrations. Share one migration set across your server, web, and mobile builds.
- The server deletes a device's cursor on any of three conditions: the device's last acknowledgement is older than the retention window, 30 days by default, the oldest change that the cursor holds back is older than that window, or the cursor holds back more changes than `maxChangesHeldForDevice` allows. That device then resyncs from a snapshot.

The `SyncControllerOptions` table is in the [configuration reference](configuration.md).
