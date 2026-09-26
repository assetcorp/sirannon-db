# Backups

Call `db.backupCapabilities()` first. The browser, Bun, and Expo drivers report `fullCopy: false`, and `db.backup` throws `BACKUP_UNSUPPORTED` on them.

## Copies to a file

- `await db.backup(path)` copies the open database while it keeps serving writes. It throws `BACKUP_ERROR` when the path already holds a file, so give each copy a new name.
- `db.scheduleBackup({ cron, destDir, maxFiles, onBackup, onError })` repeats the copy and keeps the newest `maxFiles` files, 5 by default. Upload the file in `onBackup`, which Sirannon awaits before it deletes older copies.
- The schedule keeps no process alive, so a script that only schedules a backup exits before the first copy.

## Continuous backups and a restore to a moment

1. Write a `BackupDestination` with `listPieces`, `readPiece`, and `writePiece` for the user's storage.
2. Open the database with `{ backups: { destination } }`. Sirannon takes a full copy, then captures the write-ahead log every 60 seconds by default.
3. Read `db.backupStatus()` to confirm that a run finished.
4. Restore with `restoreBackup({ destination, driver, destPath, moment })` from `@delali/sirannon-db/backup`, into a path that holds no file.

Read `backup-destinations.md` and `backup-restore.md` in the versioned documentation before you write the destination.
