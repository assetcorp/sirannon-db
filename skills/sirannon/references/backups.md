# Backups

Start with `db.backupCapabilities()`, which reports what the installed driver can copy on this machine. The browser, Bun, and Expo drivers carry no backup engine, so `backupCapabilities().fullCopy` reads `false` on them and `db.backup` throws `BACKUP_UNSUPPORTED`. A browser or phone gets its copy of the data from the server through device sync.

## A copy to a file

`db.backup(destPath)` copies an open database to a new file while reads and writes carry on, and it returns a report with the page count, the byte length, and the duration:

```ts
const report = await db.backup(`./backups/app-${Date.now()}.db`)
```

`db.backup` creates a missing directory on the way, and it throws `BACKUP_ERROR` for a path that already holds a file, so give every copy a new name.

## Copies on a schedule

`db.scheduleBackup` repeats that copy on a cron expression and deletes the oldest files beyond `maxFiles`, which defaults to 5:

```ts
db.scheduleBackup({
  cron: '0 * * * *',
  destDir: './backups',
  maxFiles: 24,
  onBackup: async report => uploadToStorage(report.destPath),
  onError: error => console.error('backup failed', error),
})
```

Sirannon awaits `onBackup` before it deletes older files, so upload the file there. The schedule's timer keeps no process alive, so a script that only schedules a backup exits before the first copy.

## Continuous backups and a restore to a moment

For storage that you supply, such as an object store, write a `BackupDestination` with `listPieces`, `readPiece`, and `writePiece`, and pass it in the `backups` option when you open the database:

```ts
const db = await sirannon.open('app', './data/app.db', {
  backups: { destination, intervalMs: 60_000 },
})
```

Sirannon then takes one full copy, and every `intervalMs` after that it captures only the write-ahead log written since the previous capture. Once the chain is `fullCopyIntervalMs` old, which defaults to 24 hours, a new full copy starts a new chain. `db.backupStatus()` reports the last run and the last error.

`restoreBackup` from `@delali/sirannon-db/backup` rebuilds a database at any moment that the chain covers:

```ts
import { restoreBackup } from '@delali/sirannon-db/backup'

await restoreBackup({ destination, driver: betterSqlite3(), destPath: './restore/app.db', moment: Date.parse('2026-09-01T12:00:00Z') })
```

`moment` counts milliseconds since the Unix epoch and defaults to now. The restore refuses a `destPath` that holds a file unless you set `replaceExisting: true`. Restore into a fresh path, then point the app at it.

On Node.js, the streamed route sends a copy without first staging it on local disk. It needs the platform package `@delali/sirannon-vfs-<platform>-<arch>`, which the package manager installs with Sirannon as an optional dependency, and `backupCapabilities().streamedCopy` reports whether the process found it.

Read `backups.md`, `backup-destinations.md`, `backup-chains.md`, and `backup-restore.md` in the versioned documentation for destinations, chain records, and deleting old pieces safely.
