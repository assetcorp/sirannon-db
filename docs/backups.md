# Backups

Sirannon copies a database while it stays open for reads and writes. It writes that copy to a file on local disk, or to storage you supply. On a database too large to copy in full every night, the `backups` option follows the first copy with only what changed since the run before it. `restoreBackup()` reads those files back and rebuilds the database from a moment you name.

The [core engine guide](core.md) covers migrations, live queries, bulk load, hooks, metrics, and the multi-tenant lifecycle.

## Taking a copy

`backup()` writes a copy to a local file. The database stays open for reads and writes throughout, because SQLite moves the pages in steps and a write runs in the gap between two of them.

```ts
await db.backup('./backups/snapshot.db')
```

`scheduleBackup()` repeats that on a cron schedule and keeps a bounded number of files:

```ts
db.scheduleBackup({
  cron: '0 */6 * * *',
  destDir: './backups',
  maxFiles: 10,
  timezone: 'America/New_York',
  onError: err => console.error('Backup failed:', err),
})
```

Both of those write to local disk. `backupTo()` sends the copy to storage you supply instead:

```ts
const report = await db.backupTo({ destination: s3Destination })

report.destinationName   // the name your storage now holds the copy under
report.bytesWritten
report.durationMs
```

## Supplying a destination

Sirannon carries no storage client, so you write three functions and it calls them. A fourth is optional, and you should write it wherever more than one node backs up to the same storage. Sirannon splits every backup into fixed-size pieces, 16 MiB by default, and numbers them:

```ts
import type { BackupDestination } from '@delali/sirannon-db'

const s3Destination: BackupDestination = {
  async writePiece(name, index, bytes) {
    await s3.send(new PutObjectCommand({ Bucket, Key: `${name}/${index}`, Body: bytes }))
  },
  async writePieceIfAbsent(name, index, bytes) {
    try {
      await s3.send(new PutObjectCommand({ Bucket, Key: `${name}/${index}`, Body: bytes, IfNoneMatch: '*' }))
      return true
    } catch (err) {
      if ((err as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 412) return false
      throw err
    }
  },
  async readPiece(name, index) {
    const object = await s3.send(new GetObjectCommand({ Bucket, Key: `${name}/${index}` }))
    return new Uint8Array(await object.Body.transformToByteArray())
  },
  async listPieces(name) {
    const pieces: { index: number; byteLength: number }[] = []
    for await (const page of paginateListObjectsV2({ client: s3 }, { Bucket, Prefix: `${name}/` })) {
      for (const object of page.Contents ?? []) {
        if (!object.Key || object.Size === undefined) continue
        pieces.push({ index: Number(object.Key.split('/').pop()), byteLength: object.Size })
      }
    }
    return pieces
  },
}
```

Sirannon relies on three properties here. Pieces arrive in any order, since SQLite writes page one last, so nothing in your code may assume piece 0 comes first. A second write to the same name and index has to replace the piece already there, because a run that stops part-way through repeats its last write when it resumes. And `listPieces` answers for the one name it receives, returning an empty list where you hold nothing under that name. S3 sends at most a thousand keys in one response, which is why the example pages through them; a listing that stopped at the first response would hide every piece past the thousandth from a restore.

`writePieceIfAbsent` is the fourth function, and it stores a piece only where that name and index hold none, reporting whether this call is the one that stored it. Sirannon keeps its list of chains under a single name, one record per chain, so two nodes that start a chain at the same moment can pick the same index for it. Where each node claims its index through this function, the storage settles which one gets it and both chains stay in the list. Where you leave the function out, Sirannon writes the record and reads it back instead, which catches the other node's write unless that write lands between those two calls, and the chain it misses drops out of the list.

Write it against whichever store you keep the backups in: S3 and R2 take `IfNoneMatch: '*'` and answer 412 where the key exists, Google Cloud Storage takes `ifGenerationMatch: 0`, Azure Blob Storage takes `If-None-Match: *`, and a local filesystem opens the file with the `wx` flag.

Sirannon gives every call to your destination ten minutes to return and then fails the run with `BACKUP_DESTINATION_ERROR`, so a storage client that hangs cannot leave a backup running forever. Pass `destinationTimeoutMs` to set a different deadline.

## How the bytes travel

Sirannon takes one of two routes. `backupCapabilities()` reports which one this process would take:

```ts
const { streamedCopy, localDiskRequired } = db.backupCapabilities()
```

The streamed route passes each piece to your destination as SQLite writes it, so a backup needs no local disk. That route uses a compiled SQLite extension that Sirannon publishes as one small package per platform, and your install step fetches only the package for the platform you are on. On a platform with no published binary, Sirannon writes one local file and sends that file on. `localDiskRequired` reads `equal-to-backup` when it does.

Node's own SQLite streams from version 23 upwards, and version 22 takes the staged route. better-sqlite3 streams once the operator sets `SQLITE_USE_URI=1` before the module loads:

```bash
SQLITE_USE_URI=1 node server.js
```

That variable turns on URI parsing for every file name the process opens. SQLite would then read a database path containing a question mark as a URI, so check the paths your application opens before you set it. Without the variable, `streamedCopy` reads `false` and every run takes the staged route.

One process loads the extension into a single SQLite build, so a streamed run through a second driver fails with an error that says so. The fingerprint costs a read of every piece back from your destination, and `fingerprint: false` skips it.

To stream on a platform Sirannon publishes no binary for, compile the extension yourself and name it:

```ts
const driver = betterSqlite3({ vfsExtensionPath: '/opt/sirannon/sirannonvfs.so' })
```

## Incremental backups

Every backup above copies the whole database, however little of it changed since the last run. On a large database, the cost of that mounts quickly.

The `backups` option copies the whole database once, then sends only what changed since the previous run. A 1 TB database that changed by 200 MB in a day uploads 200 MB.

```ts
const db = await sirannon.open('main', './data/main.db', {
  backups: {
    destination,
    intervalMs: 60_000,
    fullCopyIntervalMs: 24 * 60 * 60 * 1000,
    onError: err => pageOnCall(err),
  },
})
```

`destination` is the same object `backupTo` takes, so a destination you have already written works here unchanged. See [Supplying a destination](#supplying-a-destination).

Set `intervalMs` to how much work you can afford to lose, in milliseconds. At the default of one minute, a machine you lose takes up to a minute of writes with it.

### What gets stored

Sirannon writes one full copy, then a small file per interval holding the changes after it. Those files belong together: to rebuild the database as of some moment, you need the full copy plus every change file up to that moment, applied in order. That set is a **chain**. Sirannon records which files make up each one, so you never have to work it out from filenames.

Sirannon starts a new chain on a schedule, once a day by default, which `fullCopyIntervalMs` sets. Without that, a chain would grow all year and a restore would have to replay every file in it.

### Before you turn it on

While this option is on, Sirannon takes over one piece of SQLite housekeeping: trimming the log file SQLite keeps beside your database. Sirannon trims it immediately after each capture instead, which keeps it small.

Plan for what that costs when it stops. A cycle that fails while your app keeps writing lets the log grow until it fills the disk. A destination that stops accepting writes does that, and so do credentials that expire overnight. `onError` is your only warning, so treat anything arriving there as urgent.

`maxUncapturedLogBytes` bounds that growth in bytes. Past the figure you set, Sirannon empties the log, reports `BACKUP_CHAIN_BROKEN` through `onError`, and starts a fresh chain with a full copy on the next turn it runs. The writes that log held reach no backup, which is why it defaults to unset. PostgreSQL gives the same choice over the log a replication slot pins, through `max_slot_wal_keep_size`, and it defaults to unlimited too.

Sirannon also stages each capture beside your database file before sending it, so leave a little headroom on that volume. `stagingDir` moves it elsewhere.

### Checking on the cycle

```ts
await db.captureBackupChanges()   // run one now instead of waiting for the interval
await db.backupChain()            // every chain at the destination, newest first
```

### Backups in a replication group

Every node of a group opens with the same `backups` option. Before a turn of the cycle copies anything, Sirannon works out which node of the group takes its backups. One node finds its own identifier in that answer, while the rest stand down. A failover changes which node answers yes, but it changes no schedule.

Give it the coordinator your nodes already fail over through:

```ts
import { coordinatorBackupGroup } from '@delali/sirannon-db/replication'

const db = await sirannon.open('main', './data/main.db', {
  backups: {
    destination,
    replicationGroup: coordinatorBackupGroup({
      coordinator,
      clusterId: 'commerce-production',
      groupId: 'orders',
      nodeId: 'orders-node-a',
    }),
    onSkip: skip => log.info(skip.message),
    onError: err => pageOnCall(err),
  },
})
```

The backups go to a replica by default, which leaves the primary serving writes. `preferredNode: 'primary'` puts them on the primary instead, and `preferredNode: { nodeId: 'orders-node-c' }` pins them to one node you name. A node matching itself against a name it was given asks the coordinator nothing, so a pinned deployment keeps backing up through a coordinator outage.

A node that takes none of the backups keeps no chain of its own, though it still trims its log every turn as though it were capturing. It sends whatever capture it had staged before it lets that chain go, and where the destination refuses that capture it holds the chain, the staged frames, and the log until a later turn can send them. Once a failover brings the backups to it, its first turn copies the whole database and starts a fresh chain. That full copy is unavoidable, because two nodes hold the same rows in physically different files and a chain of change files from one node continues on no other.

A node that cannot reach its coordinator holds everything where it is. It captures nothing, trims nothing, and reports the skip, since the frames it has yet to capture are in no backup and a trim would lose them. Watch for those skips: a node partitioned for hours will grow its log for the whole partition. Set `maxUncapturedLogBytes` to bound how far that log grows.

`onSkip` receives one report for each turn this node skips. Its `reason` is `not-preferred`, `group-unavailable`, or `previous-run-active`, and its `message` is a sentence you can log as it stands. Each report also carries `uncapturedLogBytes`, the size of the write-ahead log as the node skipped, so alerting on that figure tells you a node is holding its log long before any limit ends its chain. Where a turn fails, Sirannon reports that failure through `onError`. A skipped turn still sends any capture it had staged before it stood down, so the destination can receive a piece on a skipped turn.

Sirannon reports through `onError` as the cycle starts where you give it a `replicationGroup` and a destination with no `writePieceIfAbsent`, because two of those nodes starting a chain at the same moment would lose one between them. Add the function, or give each node a `chainName` of its own.

A database opened without `replicationGroup` takes every turn, which is the answer a single-node deployment wants. Set one on every node of a group. Two nodes backing up at once write two chains into the same destination, so a restore then has to choose between them.

## Restoring to a moment

Name the moment you want back, and Sirannon rebuilds the database at a path you choose:

```ts
import { restoreBackup } from '@delali/sirannon-db/backup'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const report = await restoreBackup({
  destination,
  driver: betterSqlite3(),
  destPath: './data/recovered.db',
  moment: Date.parse('2026-08-18T09:00:00Z'),
})

report.restoresTo      // the moment the rebuilt file reflects
report.changesApplied  // change files replayed over the full copy
```

Sirannon reads the chain records at your destination and takes the newest full copy finished at or before that moment. It then replays every change file captured from that copy up to the same moment, fetching one piece and applying it before it asks for the next. One piece is therefore all a restore holds, whether the database is a gigabyte or a terabyte.

Leave `moment` out to reach the newest backup you hold. This call opens no database of its own and needs no registry, which is what lets you run it on a machine that has never seen this database.

Sirannon checks each file it fetches against the byte count and the fingerprint its backup recorded. Two kinds of gap fail the call. A chain missing a change file fails with `BACKUP_CHAIN_BROKEN` naming that file, and storage missing one of the numbered pieces a file was stored in fails with `BACKUP_DESTINATION_ERROR` naming that piece.

Sirannon assembles the rebuilt database beside the path you named and renames it onto that path once the last batch is folded in. A restore that fails, or one the machine kills part-way, therefore leaves that path holding whatever it held before. Where a database already sits at that path, Sirannon folds its write-ahead log back into it before the rename, so a machine that stops the restore between the two steps still leaves that database whole. Where the fold cannot empty that log, because another connection holds the database or SQLite cannot open the file at all, Sirannon removes that database along with its log, so a machine stopping there leaves the path plainly empty rather than quietly short of its last commits.

A database already at that path stops the call, because the rename leaves the rebuilt database there and nothing of the one it replaced. Say `replaceExisting: true` where you mean to restore over a database you no longer want:

```ts
await restoreBackup({
  destination,
  driver: betterSqlite3(),
  destPath: './data/main.db',
  replaceExisting: true,
})
```

### How much disk a restore needs

Work this out before the day you need it:

```text
free disk = the finished database + one piece + one batch of change files
```

- **The finished database** is the full copy plus everything the change files add to it.
- **One piece** is the `pieceBytes` the backup used, 16 MiB by default.
- **One batch** is `batchSize` change files, 16 by default. Sirannon writes one batch into the log beside the database and folds it in with a checkpoint. The log is empty again before the next batch begins, so the length of the chain never enters this figure.
- **The database you are replacing** counts as well where you pass `replaceExisting`, because Sirannon keeps it where it is until the rebuilt file is renamed over it.

Suppose a 200 GB database, backed up in 16 MiB pieces, capturing 40 MB of changes a minute. At the default batch size the restore would need 200 GB, plus 16 MiB, plus 640 MB of change files, which comes to roughly 200.7 GB. Restoring over the running copy of that same database would need roughly 400.7 GB, since both files sit on the disk until the rename. Lower `batchSize` where disk is tight, and raise it where a long chain spends too long checkpointing.

### Working out what a restore needs

`restoreBackup` selects the files itself, and this call shows you the same selection without fetching anything:

```ts
const plan = await db.backupRestorePlan(Date.parse('2026-08-18T09:00:00Z'))

plan.base.name    // the full copy to start from
plan.changes      // the change files to apply, in order
plan.restoresTo   // the moment you would actually reach
```

`restoresTo` is the last capture at or before the time you asked for, so at a one-minute interval you reach within a minute of it. A moment older than every full copy you still hold fails the call, rather than handing back a plan that could not work.

## Deleting old backups safely

The newest full copy is not enough on its own, because restoring to any moment after it also needs the change files in between. So ask rather than working it out by hand:

```ts
const stale = await db.backupPiecesSafeToDelete({
  restorableFrom: Date.now() - 30 * 24 * 60 * 60 * 1000,
})

for (const record of stale) await myStorage.deleteEveryPieceOf(record.name)
```

That call asks for a 30 day window, and returns everything no restore inside it needs. With no argument you get only what is already useless: a chain whose full copy someone removed, and change files stranded after a missing one.

Each record names one file. Your destination holds that file as the numbered pieces you stored under its name, so deleting a record means deleting every one of them.

Sirannon deletes nothing from your destination; it only tells you what is safe to remove.

Every `BACKUP_*` code, and the error class it arrives as, is in the [errors guide](errors.md). The normative definition is in [`packages/spec/02-core.md`](../packages/spec/02-core.md#backups).
