# Backups

Sirannon can copy a database while your application keeps reading from it and writing to it. It writes that copy to a file on local disk, or to storage that you supply. For a database that is too large to copy in full every night, the `backups` option copies the whole database once and then sends only the changes made since the previous capture. `restoreBackup()` fetches those files and rebuilds the database as it stood at a moment that you name.

Migrations, live queries, bulk load, hooks, metrics, and the multi-tenant lifecycle are in the [core engine guide](core.md).

## Taking a copy

`backup()` writes a copy to a local file. The database stays open for reads and writes throughout, because SQLite copies the pages in steps and lets a write proceed between two steps.

```ts
const report = await db.backup('./backups/snapshot.db')

report.destPath     // the absolute path of the file it wrote
report.byteLength   // the bytes that file holds
report.pageCount    // the pages SQLite moved
report.durationMs   // the milliseconds the copy took
```

`scheduleBackup()` repeats that copy on a cron schedule and keeps the number of files that `maxFiles` sets. Sirannon passes the same report to `onBackup` after each scheduled copy, so your callback receives the path of every new file without having to watch the directory:

```ts
db.scheduleBackup({
  cron: '0 */6 * * *',
  destDir: './backups',
  maxFiles: 10,
  timezone: 'America/New_York',
  onBackup: report => uploadToObjectStorage(report.destPath, report.byteLength),
  onError: err => console.error('Backup failed:', err),
})
```

Sirannon waits for `onBackup` to return before it deletes the older files and before it starts the next copy, so a file that your upload is still reading stays on disk until the callback returns. `onBackupTimeoutMs` limits that wait to ten minutes by default, and a value of zero removes the limit. Once the deadline passes, Sirannon reports a `BACKUP_ERROR` through `onError` and continues with the schedule. However, your callback may still be uploading that copy while Sirannon treats it as a file that it can delete, so set the deadline longer than your slowest upload takes.

`backup()` and `scheduleBackup()` both write to local disk, while `backupTo()` sends the copy to storage that you supply:

```ts
const report = await db.backupTo({ destination: s3Destination })

report.destinationName   // the name your storage now holds the copy under
report.bytesWritten
report.durationMs
```

## Supplying a destination

Sirannon includes no storage client, so you write three functions that Sirannon calls to store, read, and list the pieces of a backup. A fourth function, `writePieceIfAbsent`, is optional; write it whenever more than one node backs up to the same storage. Sirannon splits every backup into numbered pieces of a fixed size, which is 16 MiB by default:

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

Sirannon depends on three properties of these functions. Sirannon can send the pieces in any order, because SQLite writes page one last, so your code must never assume that piece 0 comes first. A second write to the same name and index has to replace the piece that is already stored there, because Sirannon repeats the last write when it resumes a backup that stopped part-way. `listPieces` has to return the pieces stored under the one name that it receives, and an empty list when your storage holds nothing under that name. S3 returns at most a thousand keys in one response, so the example pages through them; if your listing stopped at the first response, a restore would never reach the pieces beyond the thousandth.

`writePieceIfAbsent` stores a piece only when nothing is stored yet under that name and index, and it returns `true` when this call is the one that stored it. Sirannon stores its list of chains under a single name with one record per chain, so two nodes that start a chain at the same moment can choose the same index for their records. When each node claims its index through this function, your storage accepts one claim and rejects the other, and Sirannon moves the rejected record to the next index, so both chains stay in the list. Without the function, Sirannon writes the record and then reads it back. That check detects the other node's write unless the other write happens between those two calls, in which case the list loses one of the two chains.

Each store has its own conditional write: S3 and R2 take `IfNoneMatch: '*'` and return status 412 when the key exists, Google Cloud Storage takes `ifGenerationMatch: 0`, Azure Blob Storage takes `If-None-Match: *`, and on a local filesystem you open the file with the `wx` flag.

Sirannon allows every call to your destination ten minutes to return, and after that it stops the backup with `BACKUP_DESTINATION_ERROR`, so a storage client that hangs cannot stall a backup indefinitely. Pass `destinationTimeoutMs` to set a different deadline, or zero to remove it. When you set it in the `backups` option, the server's restore route uses the same deadline.

## How the bytes travel

Sirannon sends a full copy by one of two routes, and `backupCapabilities()` reports which route this process can use:

```ts
const { streamedCopy, localDiskRequired } = db.backupCapabilities()
```

On the streamed route, Sirannon passes each piece to your destination as SQLite writes it, so the backup needs no local disk. That route depends on a compiled SQLite extension, which Sirannon publishes as a separate package for each platform, and the install fetches only the package that matches your platform. On a platform that has no published binary, Sirannon writes a local copy first and then sends that file to your destination in pieces, which is the staged route. `localDiskRequired` is `'equal-to-backup'` on that route, because the local file is the size of the backup.

With Node's built-in SQLite, Sirannon streams on Node 23 and later, and it takes the staged route on Node 22. With better-sqlite3, Sirannon streams only when you set `SQLITE_USE_URI=1` before the module loads:

```bash
SQLITE_USE_URI=1 node server.js
```

That variable turns on URI parsing for every file name that the process opens. SQLite then reads any database path that begins with `file:` as a URI, so check that none of the paths that your application opens begins with `file:` before you set it. Without the variable, `streamedCopy` is `false`, and every full copy takes the staged route.

A process can load the extension into only one SQLite build, so a streamed backup through a second driver in the same process fails with an error. On the streamed route, Sirannon computes the fingerprint by reading every piece back from your destination, which adds a download of the whole backup, and `fingerprint: false` skips that read.

To stream on a platform that has no published binary, compile the extension yourself and pass its path to the driver:

```ts
const driver = betterSqlite3({ vfsExtensionPath: '/opt/sirannon/sirannonvfs.so' })
```

## Incremental backups

Every backup above copies the whole database, however little of it has changed, so on a large database each copy transfers every byte again.

The `backups` option copies the whole database once and then, on each interval, sends only the changes made since the previous capture. Between two full copies, Sirannon uploads only the pages that your writes changed, so a 1 TB database that writes 200 MB to its log in an hour uploads about 200 MB for that hour.

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

`destination` takes the same object as `backupTo`, so a destination that you have already written works here unchanged, as [Supplying a destination](#supplying-a-destination) describes.

Set `intervalMs`, in milliseconds, to the span of writes that you can afford to lose. At the default of one minute, if you lose the machine, you can lose up to a minute of writes with it.

### What gets stored

Sirannon writes one full copy and then, on each interval, a change file that holds the writes made since the previous file. To rebuild the database as it stood at some moment, you need the full copy and every change file up to that moment, applied in order. Sirannon calls that set of files a **chain**, and it records which files make up each chain, so you never have to work that out from file names.

Sirannon starts a new chain with a fresh full copy once a day by default, and `fullCopyIntervalMs` sets that interval. Without a new chain, one chain would keep growing, and a restore would have to replay every file in it.

Every report records where its file comes in the chain. For a change file, `position` gives the stretch of the log that the file holds, and for a full copy, `logPosition` gives the point that the log had reached when the copy finished. Both include the two salts that SQLite writes into the log, and SQLite changes that pair each time it restarts the log, so the salts identify one generation of the log.

Sirannon checkpoints the log after each capture, but no checkpoint happens between a full copy and the first change file in its chain, so those two files come from the same generation of the log and report the same salts. A checkpoint that empties the log restarts it, so the change file that Sirannon captures after that checkpoint reports the new salts. An open read can stop a checkpoint from emptying the log, in which case the log stays in the same generation, so two consecutive change files can report the same salts. A restore orders the files by position and ignores the salts, so you need them only when you want to know which generation of the log a file came from.

### Before you turn it on

While this option is on, Sirannon turns off SQLite's automatic checkpoint, which empties the write-ahead log file beside your database, and it checkpoints that log itself immediately after each capture, so the log stays small while the cycle keeps up.

Plan for the case where the cycle stops. If the cycle keeps failing while your application keeps writing, the log grows until it fills the disk. A destination that starts rejecting writes can cause that, and so can credentials that expire. `onError` delivers each failure as it happens, so treat every error that it receives as urgent.

`maxUncapturedLogBytes` sets a limit, in bytes, on that growth. Once the log grows past that limit, Sirannon empties it and reports `BACKUP_CHAIN_BROKEN` through `onError`, and the next turn that can proceed starts a fresh chain with a full copy. The writes that the log held are then in no backup, which is why the option has no default limit. PostgreSQL offers the same choice through `max_slot_wal_keep_size`, which limits the log that a replication slot retains, and its default is unlimited as well.

Sirannon also stages each capture in a directory beside your database file before it sends the capture, so leave free space on that volume for a capture, or set `stagingDir` to put the staging directory on another volume.

### Checking on the cycle

```ts
await db.captureBackupChanges()   // run one now instead of waiting for the interval
await db.backupChain()            // every chain at the destination, newest first
db.backupStatus()                 // what the cycle is doing at this moment
```

You can call `backupStatus()` at any time, so you can check the cycle without having to catch each callback as it fires. Its `running` field shows whether a turn is under way, and during a turn, `progress` gives the pages that the copy still has to move and the bytes that Sirannon has stored at the destination so far. On the staged route, `phase` changes to `'transfer'` once the copy finishes and `remainingPages` stays at zero from then on, so follow `bytesWritten` from that point. On the streamed route, Sirannon sends pieces while the copy is still moving pages, so `'copy'` and `'transfer'` reports alternate, and a `'transfer'` report can still give pages left to move. The status also includes `lastRun`, `lastSkip`, and `lastError`, which hold the most recent turn that wrote something, the most recent skipped turn, and the most recent failure. Pass an `onProgress` callback in the `backups` option when you want every step, and call `backupStatus()` when you want the latest figures.

When a turn fails, `lastError` records the error code, the message, and how far the turn had got. `lastError.chainId` identifies the chain that the turn was extending, and `lastError.durationMs` gives the milliseconds from the start of the turn to the failure. `lastError.progress` gives the run identifier and the number of pieces and bytes that Sirannon had stored at your destination before the failure. From that last figure, you can tell a destination that rejected the first piece from one that rejected the last.

### Checking a backup is still readable

Without a check, you would discover a damaged piece only after a restore had begun. `verifyBackup()` reads the backup back ahead of time, so it reports the same damage while your database is still intact:

```ts
const chains = await db.backupChain()
const fullCopy = chains[0]?.base
if (fullCopy) {
  const checked = await db.verifyBackup(fullCopy.name)
  log.info(`${checked.pieceCount} pieces, ${checked.bytesRead} bytes read`)
}
```

Sirannon fetches every piece in order and computes a SHA-256 digest over the bytes as it receives them. It then compares that digest and the byte count with the ones that the backup recorded when it wrote the pieces. Sirannon holds one piece in memory at a time and writes none of them to disk, so checking even a terabyte backup needs no local storage. The call fails with `BACKUP_DESTINATION_ERROR` when a piece is missing, when the byte count differs from the recorded one, or when the digest differs from the recorded one. If you turned fingerprinting off, Sirannon compares only the piece listing and the byte count, and the result includes no fingerprint.

### Backups in a replication group

Open every node of a group with the same `backups` option. At the start of each turn, before it copies anything, Sirannon works out which node of the group takes the backups. Every node computes that answer from the same group membership, so one node takes the turn while the others skip it. A failover can change which node takes the backups, but the schedule stays the same.

Pass the same coordinator that your nodes use for failover:

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

By default, a replica takes the backups and leaves the primary to serve writes, and a group with no replica falls back to its primary. `preferredNode: 'primary'` puts the backups on the primary, and `preferredNode: { nodeId: 'orders-node-c' }` pins them to the node that you name. With a pinned node, each node compares its own identifier with that name and makes no call to the coordinator, so a pinned deployment keeps backing up during a coordinator outage.

A node that takes none of the backups keeps no chain of its own, although it still checkpoints its log every turn. Before it gives up a chain, it sends any capture that it had staged. If the destination rejects that capture, the node keeps the chain, the staged capture, and the log until a later turn can send them. When a failover moves the backups to this node, its first turn copies the whole database and starts a new chain. Sirannon needs that full copy because each node stores the same rows in a physically different file, so change files captured on one node cannot extend a chain from another.

A node that cannot read its group's membership from the coordinator leaves its chain and its log as they are. It skips the turn without capturing or checkpointing, because the frames that it has yet to capture are in no backup and a checkpoint would discard them. Alert on those skips, because a node that stays cut off from the coordinator keeps growing its log for as long as the partition lasts. Set `maxUncapturedLogBytes` to limit that growth.

Sirannon calls `onSkip` once for each turn that this node skips. The report's `reason` is `not-preferred`, `group-unavailable`, or `previous-run-active`, and its `message` is a sentence that you can log as it is. The report also gives `uncapturedLogBytes`, the size of the write-ahead log when the node skipped the turn, so an alert on that figure can warn you that a node is holding its log well before `maxUncapturedLogBytes` ends the chain. Sirannon reports a failed turn through `onError`, not through `onSkip`. On a `not-preferred` skip, the node still sends any capture that it had staged before it gives up the chain, so your destination can receive a piece during a skipped turn.

When you give the cycle a `replicationGroup` and a destination without `writePieceIfAbsent`, Sirannon reports a `BACKUP_DESTINATION_ERROR` through `onError` as the cycle starts, because two nodes that start a chain at the same moment could lose one of the two chains. Add the function, or give each node a `chainName` of its own.

A database that you open without `replicationGroup` takes every turn, which suits a single-node deployment. In a group, set it on every node, because otherwise two nodes can back up at once and write two chains to the same destination, and a restore then takes whichever chain has the newest full copy before your moment.

## Restoring to a moment

`restoreBackup()` rebuilds the database as it stood at the moment that you name, and it writes the result to a path that you choose:

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

Sirannon reads the chain records at your destination and takes the newest full copy that finished at or before that moment. It then replays every change file captured after that copy up to the same moment, and it applies each piece before it fetches the next. A restore therefore holds one piece in memory at a time, whatever the size of the database.

Leave out `moment` to restore to the newest backup at your destination. The call needs no registry and no open database, so you can call it on a machine that has never opened this database.

Sirannon checks each file that it fetches against the byte count and the fingerprint that its backup recorded. The call also fails on two kinds of gap. When a chain lacks a change file, the call fails with `BACKUP_CHAIN_BROKEN` and names that file, and when your storage lacks one of a file's numbered pieces, it fails with `BACKUP_DESTINATION_ERROR` and names that piece.

Sirannon builds the database in a file beside the path that you named, and it renames that file onto your path once it has applied the last batch. If the restore fails, or the machine stops part-way, your path therefore still holds whatever it held before. If a database already exists at that path, Sirannon checkpoints its write-ahead log into it before the rename, so that database stays whole if the machine stops between the two steps. If the checkpoint cannot empty that log, because another connection has the database open or SQLite cannot open the file at all, Sirannon deletes that database and its log. A machine that stops at that point leaves the path empty, which shows plainly that the restore did not finish.

By default, the call fails when a database already exists at that path, because the rename replaces that database completely. Pass `replaceExisting: true` when you mean to restore over a database that you no longer need:

```ts
await restoreBackup({
  destination,
  driver: betterSqlite3(),
  destPath: './data/main.db',
  replaceExisting: true,
})
```

### How much disk a restore needs

Work this figure out before you need to restore:

```text
free disk = the finished database + one piece + one batch of change files
```

- **The finished database** is the full copy plus everything that the change files add to it.
- **One piece** is the `pieceBytes` value that the backup used, which is 16 MiB by default.
- **One batch** is `batchSize` change files, which is 16 by default. Sirannon writes one batch into the log beside the database and then checkpoints it into the database. The log is empty again before the next batch starts, so the length of the chain has no effect on this figure.
- **The database that you are replacing** counts as well when you pass `replaceExisting`, because Sirannon leaves it in place until it renames the rebuilt file over it.

Suppose that you back up a 200 GB database in 16 MiB pieces and that Sirannon captures 40 MB of changes from it every minute. At the default batch size, the restore would need 200 GB, plus 16 MiB, plus 640 MB of change files, which comes to roughly 200.7 GB. Restoring over the live copy of that same database would need roughly 400.7 GB, since both files are on the disk until the rename. Lower `batchSize` when disk space is tight, and raise it when a long chain spends too long on checkpoints. The largest value is 4096, which at the default one-minute interval covers close to three days of change files, longer than the one day that a chain lasts by default before a fresh full copy replaces it.

### Working out what a restore needs

`restoreBackup` selects the files itself, and `backupRestorePlan()` shows you the same selection without fetching any backup piece:

```ts
const plan = await db.backupRestorePlan(Date.parse('2026-08-18T09:00:00Z'))

plan.base.name    // the full copy to start from
plan.changes      // the change files to apply, in order
plan.restoresTo   // the moment you would actually reach
```

`restoresTo` is the time of the last capture at or before the moment that you asked for, so at a one-minute interval the restore reaches a point within a minute of that moment. A moment older than every full copy at your destination fails the call with `BACKUP_CHAIN_BROKEN`, and the error message gives the earliest moment that you can restore.

## Deleting old backups safely

A restore to a moment after the newest full copy also needs the change files that follow that copy, so ask Sirannon which files are safe to delete:

```ts
const stale = await db.backupPiecesSafeToDelete({
  restorableFrom: Date.now() - 30 * 24 * 60 * 60 * 1000,
})

for (const record of stale) await myStorage.deleteEveryPieceOf(record.name)
```

That call asks to keep a 30-day window, and it returns every record that no restore inside the window needs. Without an argument, it returns only the records that no restore can ever use: the change files of a chain whose full copy is gone, and the change files that follow a gap in a chain.

Each record names one file. Your destination stores that file as numbered pieces under its name, so to delete a record, delete every one of those pieces.

Sirannon lists these records and leaves the deletion to you.

The [errors guide](errors.md) lists every `BACKUP_*` code with its error class. The normative definition is in [`packages/spec/02-core.md`](../packages/spec/02-core.md#backups).

## Reaching backups over the server

An operator can start a backup, read its progress, list the backups at the destination, check one stored backup, and ask which files are safe to delete, all through server routes that your server authenticates like any other route. The server can also restore a database through a route that stays closed until you set `acceptBackupRestore`. The [server guide](server.md#backup-routes) lists the routes and the response from each one.
