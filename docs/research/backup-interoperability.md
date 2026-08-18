# Backup interoperability: what other systems do

This file collects evidence for the design conversation on [issue #36](https://github.com/assetcorp/sirannon-db/issues/36), which asks whether writing a full copy to a local file path is the right shape when the deployment has little spare disk and the operator wants the bytes in object storage. Its scope is what other systems do, and the design decision stays open. Every claim below names the document or the source file it came from.

This repository keeps guides in `docs/` and decision records in `docs/adr/`, so `docs/research/` is a proposed location for a note of this kind.

## What Sirannon does today

`BackupManager.backup` in `packages/ts/src/core/backup/backup.ts` runs one statement, `VACUUM INTO '<path>'`, on the connection it is given (line 47). It refuses a destination that already exists (line 40), creates the parent directory (line 30), and deletes a partial file when the statement fails (line 50). `BackupEngine` in `packages/ts/src/core/driver/types.ts` (lines 34 to 43) declares two members, `backup(conn, destPath)` and `schedule(conn, options, runExclusive)`, so a filesystem path is the only destination the driver contract admits.

`DatabaseBackupController.backup` in `packages/ts/src/core/database-backup.ts` (line 28) wraps the call in `runExclusive`. `packages/ts/src/core/database-create.ts` binds `runExclusive` to `op => writerLock.run(op)` and the connection to `pool.acquireWriter()` (lines 75 to 79), and the same `writerLock` is passed to the CDC and sync controllers (lines 81 to 92). A backup therefore blocks every other Sirannon write in the process for its whole duration, including replication apply.

`nodeBackupEngine` in `packages/ts/src/drivers/node-runtime.ts` is the only implementation, and `betterSqlite3` and `nodeSqlite` both register it through `createBackupEngine`. The scheduler in `packages/ts/src/core/backup/scheduler.ts` names each file `backup-<timestamp>.db`, deletes older files past `maxFiles` by modification time (line 165), and calls `onError` on failure (line 168). No callback fires on success, which is the gap the issue describes.

## 1. Producing bytes without a local file

### SQLite's online backup API

The destination is a database connection rather than a path. `sqlite3_backup_init(D,N,S,M)` takes `D` as "the database connection associated with the destination database" and `N` as the schema name, with `S` and `M` naming the source connection and schema, according to the [backup API reference](https://sqlite.org/c3ref/backup_finish.html). The two connections must differ, and the call fails returning NULL if a read or read-write transaction is already open on the destination.

`sqlite3_backup_step(B,N)` copies up to N pages and returns `SQLITE_OK` while pages remain, or `SQLITE_DONE` when it has copied them all. A negative N copies all remaining pages in one call. The same reference gives the restart rule: "If the source database is modified by an external process or via a database connection other than the one being used by the backup operation, then the backup will be automatically restarted by the next call to sqlite3_backup_step(). If the source database is modified by using the same database connection as is used by the backup operation, then the backup database is automatically updated at the same time."

Both halves of that rule are visible in the source. `sqlite3BackupRestart` in `src/backup.c` sets `p->iNext = 1`, returning the copy to page one, and its comment states that the pager calls it "when the pager layer detects that the database has been modified by an external database connection", because "there is no way of knowing which of the pages that have been copied into the destination database are still valid". `pager_reset` in `src/pager.c` calls it when it discards the page cache. The other half is `backupUpdate` in `src/backup.c`, which copies a freshly written page into the destination when `iPage < p->iNext`, so a page already copied is patched in place.

The [backup documentation](https://sqlite.org/backup.html) states the consequence plainly in section 3.1: "If the backup process is restarted frequently enough it may never run to completion and the backupDb() function may never return."

Because the destination is a handle, the VFS behind that handle determines where the bytes are written. A registered VFS is selectable through the `vfs=NAME` URI query parameter, which "causes the database connection to be opened using the VFS called NAME" and fails if NAME was not built in or registered with `sqlite3_vfs_register()`, per section 3.1 of the [URI filename documentation](https://sqlite.org/uri.html). URI filenames are off by default and are enabled by `SQLITE_USE_URI=1`, by `sqlite3_config(SQLITE_CONFIG_URI,1)`, or per connection by `SQLITE_OPEN_URI`, per section 2 of the same page.

### VACUUM INTO

The destination is a filename, and "the file named by the INTO clause must not previously exist, or else it must be an empty file, or the VACUUM INTO command will fail with an error", according to the [VACUUM documentation](https://sqlite.org/lang_vacuum.html). The same page states that the argument "can be a URI filename if URI filenames are enabled", and that the output is "a consistent snapshot of the original database".

URI filenames apply there because of how the target is opened. `sqlite3RunVacuum` in `src/vacuum.c` opens the output with `execSqlF(db, pzErrMsg, "ATTACH %Q AS %s", zOut, zDbVacuum)` (line 226), and the URI page states that URI filenames are recognised on ATTACH whenever they were recognised at open. The same function then parses a query parameter from the attached filename, `sqlite3_uri_int64(zFilename, "reserve", nRes)` (line 255), which confirms that URI parsing applies to the target. So `vfs=NAME` on a VACUUM INTO target is within what the code parses, though I did not run it.

The documentation compares the two mechanisms directly: "The advantage of using VACUUM INTO is that the resulting backup database is minimal in size and hence the amount of filesystem I/O may be reduced. Also, all deleted content is purged from the backup, leaving behind no forensic traces. On the other hand, the backup API uses fewer CPU cycles and can be executed incrementally."

### sqlite3_rsync

`sqlite3_rsync ORIGIN REPLICA` makes REPLICA a copy of ORIGIN, where either side may be `USER@HOST:PATH` and ssh connects the two ends, per the [sqlite3_rsync documentation](https://sqlite.org/rsync.html). REPLICA becomes "a fully-consistent snapshot of ORIGIN at an instant in time", specifically the instant the command started.

What crosses the wire is hashes and pages rather than a file: "the replica sends cryptographic hashes for pages or groups of pages over to the origin side. The origin sends back page content that differs, or requests finer-grain hashes if a multi-page hash does not match." When the two sides start out similar, "the total bandwidth is often less than 0.01% of the database size. In tests, a 500MB database will typically synchronize with about 20KB of network traffic."

Each end needs the binary. The documentation states that on the remote system "this utility must be installed in one of the directories in the default $PATH for SSH", with `--exe` to name another location, and that "at least one of ORIGIN or REPLICA must be on the local machine". Both ends had to be in WAL mode with the same page size until version 3.50.0 (2025-05-29) removed both limitations.

The pipe is an interface of its own, and the source shows it. The comment above `main()` in `tool/sqlite3_rsync.c` (lines 2046 to 2066) lists four input forms, of which two are the internal ones: `sqlite3_rsync --origin FILENAME1` and `sqlite3_rsync --replica FILENAME2`. "The user types (1) or (2). SSH launches (3) or (4). ... If (3) is seen, call originSide() on stdin and stdout. If (4) is seen, call replicaSide() on stdin and stdout." Both ends still need an SQLite database file, so this moves a database to a machine rather than a byte stream to a bucket.

### What the two Node drivers expose

better-sqlite3's `.backup(destination, [options])` takes a path and returns a promise, per its [API documentation](https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md). Its `progress` callback receives `.totalPages` and `.remainingPages`, "100 pages will be transferred after each cycle of the event loop" by default, and returning a number from the callback changes that rate, with `0` pausing the backup. Throwing from the callback aborts it. Its documentation repeats SQLite's restart rule and adds an operational recommendation: "it's recommended that only a single connection is responsible for mutating the database if online backups are being performed."

`node:sqlite` exposes `sqlite.backup(sourceDb, path[, options])`, added in v23.8.0 and v22.16.0, where `path` is a string, Buffer, or URL, and "if the file already exists, the contents will be overwritten", per the [Node SQLite documentation](https://nodejs.org/api/sqlite.html). Its options are `source`, `target`, `rate` (default 100 pages), and `progress`. It resolves with the total number of backed-up pages. The page states that the method "abstracts the sqlite3_backup_init(), sqlite3_backup_step() and sqlite3_backup_finish() functions".

Both also expose a whole-database serialisation. better-sqlite3's `.serialize()` returns a Buffer, and `node:sqlite` added `database.serialize()` returning a `Uint8Array` in v26.1.0. `sqlite3_serialize` underneath returns "a pointer to memory that is a serialization of the S database", and "for an ordinary on-disk database file, the serialization is just a copy of the disk file", per the [serialize reference](https://sqlite.org/c3ref/serialize.html). A 170 GB database in one buffer is not usable, so both drivers offer a filesystem path and nothing else. Neither documented API surface includes VFS registration.

## 2. Reporting what a finished backup produced

### PostgreSQL's backup manifest

The manifest is a JSON document with five top-level keys, per the [manifest format documentation](https://www.postgresql.org/docs/current/backup-manifest-toplevel.html): `PostgreSQL-Backup-Manifest-Version`, which is 2 from PostgreSQL 17 and 1 before; `System-Identifier`, the database system identifier, present only at version 2; `Files`; `WAL-Ranges`; and `Manifest-Checksum`. That last key "is always present on the last line" and holds "a SHA-256 checksum of all the preceding lines. We use a fixed checksum method here to make it possible for clients to do incremental parsing of the manifest."

Each file object contains `Path` or `Encoded-Path`, plus `Size` and `Last-Modified`, which are always present, per the [file object documentation](https://www.postgresql.org/docs/current/backup-manifest-files.html). That page is explicit that `Last-Modified` "is not used by pg_verifybackup. It is included only for informational purposes." When file checksums are enabled, `Checksum-Algorithm` and `Checksum` follow, and the supported algorithms are CRC32C, SHA224, SHA256, SHA384, and SHA512. The protocol default is `MANIFEST_CHECKSUMS CRC32C`, per the [streaming replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html).

Each WAL range object contains `Timeline`, `Start-LSN`, and `End-LSN`, where `End-LSN` is "the earliest LSN at which replay on the indicated timeline may end when making use of this backup", per the [WAL range documentation](https://www.postgresql.org/docs/current/backup-manifest-wal-ranges.html).

`pg_verifybackup` runs four stages against that manifest, per its [reference page](https://www.postgresql.org/docs/current/app-pgverifybackup.html). It parses the manifest and fails if it is malformed, if the system identifier does not match `pg_control`, or if the manifest fails its own internal checksum. It then compares the files on disk against the file list, detecting extra and missing files. It then recomputes and compares checksums. It finally invokes `pg_waldump` to confirm the WAL records the manifest names can be read and parsed. The page states its own limit: "the validation which is performed by pg_verifybackup does not and cannot include every check which will be performed by a running server when attempting to make use of the backup."

### What SQLite exposes about progress

SQLite exposes two functions and nothing else. `sqlite3_backup_remaining()` "returns the number of pages still to be backed up at the conclusion of the most recent sqlite3_backup_step()", and `sqlite3_backup_pagecount()` "returns the total number of pages in the source database" at that same point, per the [backup API reference](https://sqlite.org/c3ref/backup_finish.html). Both are stale by design, since "the values returned by these functions are only updated by sqlite3_backup_step()", so a change to the source size is invisible until the next step. Section 3.2 of the [backup documentation](https://sqlite.org/backup.html) gives the completion formula as `100% * (pagecount() - remaining()) / pagecount()` and repeats that the two functions "report values stored by the previous call to sqlite3_backup_step(), they do not actually inspect the source database file".

Both are also documented as not strictly threadsafe: "If they are invoked at the same time as another thread is invoking sqlite3_backup_step() it is possible that they return invalid values."

VACUUM INTO exposes neither. Its documentation describes no progress interface at all, and the statement returns once.

PostgreSQL's progress reporting at the protocol level is opt-in and carries a stated cost. `PROGRESS` "will send back an approximate size in the header of each tablespace", and that size "is calculated by enumerating all the file sizes once before the transfer is even started, and might as such have a negative impact on the performance. In particular, it might take longer before the first data is streamed." The size is approximate because the files change during the backup.

## 3. Avoiding a full copy every time

### PostgreSQL 17 incremental base backup

`pg_basebackup --incremental=<old manifest file>` takes the manifest of an earlier backup from the same server, per the [continuous archiving documentation](https://www.postgresql.org/docs/current/continuous-archiving.html). In the result, "non-relation files will be included in their entirety, but some relation files may be replaced by smaller incremental files which contain only the blocks which have been changed since the earlier backup and enough metadata to reconstruct the current version of the file". At the protocol level the client sends `UPLOAD_MANIFEST` first and then `BASE_BACKUP (INCREMENTAL)`.

The server selects the blocks to send using WAL summaries in `pg_wal/summaries`. "The summaries present in this directory must cover all LSNs from the start LSN of the prior backup to the start LSN of the current backup", and the backup fails when they are absent. The server waits for summaries that have not been written yet, which "also helps if the WAL summarization process has fallen behind".

The summariser is a separate process controlled by `summarize_wal`, which defaults to off, and `wal_summary_keep_time`, which defaults to 10 days, per the [WAL configuration documentation](https://www.postgresql.org/docs/current/runtime-config-wal.html). The documentation warns that "WAL summaries must be available for the entire range of WAL records between the preceding backup and the new one being taken; if not, the incremental backup will fail."

The [pg_walsummary reference](https://www.postgresql.org/docs/current/app-pgwalsummary.html) documents what a summary file contains: "A WAL summary file is indexed by tablespace OID, relation OID, and relation fork. For each relation fork, it stores the list of blocks that were modified by WAL within the range summarized in the file. It can also store a 'limit block', which is 0 if the relation fork was created or truncated within the relevant WAL range, and otherwise the shortest length to which the relation fork was truncated". So the summary is a per-fork set of changed block numbers rather than a copy of any data.

`pg_combinebackup` reconstructs "a synthetic full backup from an incremental backup and the earlier backups upon which it depends", taking the chain on the command line from oldest to newest, per its [reference page](https://www.postgresql.org/docs/current/app-pgcombinebackup.html). Its output can serve as an input to a later run. It "only attempts to verify that the backups have the correct relationship to each other, not that each individual backup is intact", which is why `pg_verifybackup` remains a separate tool. That page also states that "PostgreSQL has no built-in mechanism to figure out which backups are still needed as a basis for restoring later incremental backups".

### Litestream

Litestream "continuously copies write-ahead log pages from disk to a replica", per its [how-it-works page](https://litestream.io/how-it-works/). It reads those pages by controlling when checkpoints run: "It starts a long-running read transaction to prevent any other process from checkpointing and restarting the WAL file. Instead, it continually reads new WAL pages and manually calls out to SQLite to perform checkpoints as necessary."

The unit it stores is an LTX file holding new WAL pages, tagged with a monotonically increasing transaction ID and with checksums alongside the pages. A TXID "identifies the whole batch of WAL pages in that file, which may span one or more SQLite write transactions, so it is not a per-transaction identifier". When pending WAL exceeds `max-sync-wal-bytes`, 64 MiB by default, one catch-up splits across several files, and the check happens only after a WAL commit marker "so a batch never ends mid-transaction".

Between snapshots it stores a tiered set: L0 holds uncompacted per-sync batches, L1, L2, and L3 are merged every 30 seconds, 5 minutes, and 1 hour by default, and the snapshot level is "a full copy of the database, created every 24 hours by default". Restore "fetches the most recent snapshot that does not overshoot the requested restore point and then applies each subsequent LTX file in TXID order". Because TXIDs form a contiguous sequence, a gap is detectable before the restore starts. Litestream "replays whole LTX files and never applies part of one", so restore points are file boundaries, and they coarsen as retention removes L0 files after `l0-retention`, 5 minutes by default.

### A storage-level precedent outside PostgreSQL

Percona XtraBackup keys its incremental backups on a per-page version number, per its [incremental backup documentation](https://docs.percona.com/percona-xtrabackup/latest/create-incremental-backup.html): "Incremental backups work because each InnoDB page contains a log sequence number, or LSN. The LSN is the system version number for the entire database. Each page's LSN shows how recently it was changed. An incremental backup copies each page which LSN is newer than the previous incremental or full backup's LSN. An algorithm finds the pages that match the criteria. The algorithm reads the data pages and checks the page LSN".

The chain metadata is a small text file. `xtrabackup_checkpoints` records `backup_type`, `from_lsn`, `to_lsn`, and `last_lsn`, and "from_lsn is the starting LSN of the backup and for incremental it has to be the same as to_lsn (if it is the last checkpoint) of the previous/base backup". The result of an incremental run is a set of `.delta` files, such as `ibdata1.delta`.

### What these need that a logical change log cannot supply

All three approaches key on a physical unit and a monotonic position: PostgreSQL on a block number within a relation fork plus an LSN, XtraBackup on an InnoDB page plus its LSN, and Litestream on a WAL frame plus a TXID. Three requirements follow. The first is a stable physical addressing unit whose identity holds between two backups. The second is a durable record of which units changed between two positions, retained long enough to span the interval, which is why PostgreSQL keeps summaries for 10 days by default and why Litestream holds a read transaction so that the WAL is not recycled underneath it. The third is reconstruction that never interprets application semantics, which is what lets `pg_combinebackup` produce a byte-level correct file without a running server.

Sirannon's change log supplies none of the three. `installCdcTriggers` in `packages/ts/src/core/cdc/trigger-sql.ts` creates AFTER INSERT, UPDATE, and DELETE triggers that write `table_name`, `operation`, `row_id`, `old_data`, and `new_data` into `_sirannon_changes` as JSON. Capture is opt-in per table through `ChangeTracker.watch`, and `_sirannon` identifiers are refused outright, so an unwatched table produces no rows at all. `DEFAULT_RETENTION_MS` in `packages/ts/src/core/cdc/change-tracker.ts` is 3,600,000, one hour, against the ten days PostgreSQL keeps its WAL summaries by default. The log therefore contains nothing about pages, nothing about tables added before `watch` was called, nothing about indexes, views, or free-list state, and nothing about any change older than an hour. Because the triggers fire per row, a bulk delete costs one log row per deleted row where a page-level scheme would record one changed page.

The change log supplies what none of the physical schemes can. It names the table and the column, and it holds the before and after values, so a restore can cover one table rather than the whole file, and the stream stays portable across page sizes and SQLite versions. The PostgreSQL documentation states the same limit from the other side: "pg_dump and pg_dumpall do not produce file-system-level backups and cannot be used as part of a continuous-archiving solution. Such dumps are logical and do not contain enough information to be used by WAL replay".

## 4. Consistency without blocking writes

### The online backup API

The API takes two locks on different schedules. On the destination, "SQLite holds a write transaction open on the destination database file for the duration of the backup operation", and more precisely, "the first call to sqlite3_backup_step() obtains an exclusive lock on the destination file. The exclusive lock is not released until either sqlite3_backup_finish() is called or the backup operation is complete and sqlite3_backup_step() returns SQLITE_DONE", per the [backup API reference](https://sqlite.org/c3ref/backup_finish.html).

On the source, "every call to sqlite3_backup_step() obtains a shared lock on the source database that lasts for the duration of the sqlite3_backup_step() call", and "the source database is read-locked only while it is being read; it is not locked continuously for the entire backup operation". Between steps nothing is held: "during the 250 ms sleep in step 3 above, no read-lock is held on the database file and the mutex associated with pDb is not held", per section 3.1 of the [backup documentation](https://sqlite.org/backup.html).

A concurrent writer on another connection is never blocked, because the backup restarts from page one instead. A concurrent write on the backup's own connection is cheap, because `backupUpdate` patches the already-copied page. The documentation quantifies the difference: writes from another connection "are significantly more expensive than writes made to a file-based source database using pDb (as the entire backup operation must be restarted in the former two cases)". A writer holding the source connection at the moment of a step call gets a different outcome: "If the source database connection is being used to write to the source database when sqlite3_backup_step() is called, then SQLITE_LOCKED is returned immediately."

### VACUUM INTO

The source is under a read transaction for the whole copy, not a write transaction. `sqlite3RunVacuum` in `src/vacuum.c` calls `execSql(db, pzErrMsg, "BEGIN")` and then `sqlite3BtreeBeginTrans(pMain, pOut==0 ? 2 : 0, 0)` at line 270. The write flag is `2` for a plain VACUUM and `0` for a VACUUM INTO, because `pOut` is non-null in the INTO case. That matches the documentation: "VACUUM (but not VACUUM INTO) is a write operation and so if another database connection is holding a lock that prevents writes, then the VACUUM will fail".

So a concurrent writer on another connection is not blocked by a VACUUM INTO, and unlike the backup API it does not force a restart either, because the reader holds one snapshot throughout. That same held snapshot, however, stops the checkpointer. In WAL mode "a checkpoint can run concurrently with readers, however the checkpoint must stop when it reaches a page in the WAL that is past the end mark of any current reader", and "thus a long-running read transaction can prevent a checkpointer from making progress", per section 2.2 of the [WAL documentation](https://sqlite.org/wal.html). A VACUUM INTO of a 170 GB database therefore pins the WAL for as long as the copy runs, and every write committed in that window accumulates in the WAL file, on the same disk that had no room for the copy.

Two further conditions apply to the caller. `sqlite3RunVacuum` rejects the statement with "cannot VACUUM from within a transaction" when `db->autoCommit` is clear (line 169), and with "cannot VACUUM - SQL statements in progress" when `db->nVdbeActive > 1` (line 173). The documentation states the same: "Unfinalized SQL statements typically hold a read transaction open, so the VACUUM might fail if there are unfinalized SQL statements on the same connection."

In Sirannon, the writer lock supplies both conditions, since `DatabaseBackupController.backup` runs inside `writerLock.run` on the pooled writer connection. The price is that the same lock gates the CDC and sync controllers, so a long backup stalls replication apply as well as user writes. Reads are served from the read pool, which the writer lock does not gate.

### sqlite3_rsync

The origin opens the database read-write, runs `BEGIN`, then reads `PRAGMA page_count` and `PRAGMA page_size` (`tool/sqlite3_rsync.c`, `originSide`, lines 1385 to 1401), so a read transaction covers the run. The replica runs `BEGIN IMMEDIATE` (`replicaSide`, line 1853), a write transaction, held while the origin sends pages.

The documentation matches the code on both sides: "Other programs can write to ORIGIN and can read from REPLICA while this utility runs", and "While sqlite3_rsync is running, REPLICA is read-only. Queries can be run against REPLICA while this utility is running, just not write transactions". Because the origin holds one read transaction throughout, it pins the origin's WAL for the duration exactly as a VACUUM INTO does.

### Litestream and PostgreSQL

Litestream holds a read transaction for the process lifetime rather than for one backup, because that is what stops another process checkpointing and restarting the WAL underneath it. It then drives checkpoints itself, which is how it bounds the WAL it has pinned.

PostgreSQL's `BASE_BACKUP` puts the cluster into backup mode automatically: "The system will automatically be put in backup mode before the backup is started, and taken out of it when the backup is complete." The `CHECKPOINT` option selects `'fast'` or `'spread'`, defaulting to `'spread'`, per the [streaming replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html). An inconsistent file copy is acceptable there, and the [continuous archiving documentation](https://www.postgresql.org/docs/current/continuous-archiving.html) says why: "We do not need a perfectly consistent file system backup as the starting point. Any internal inconsistency in the backup will be corrected by log replay".

## 5. Streaming as an interface

### pg_basebackup on the command line

`pg_basebackup` writes the archive to standard output when the caller passes a dash as the target directory: "When the backup is in tar format, the target directory may be specified as - (dash), causing the tar file to be written to stdout." The restriction is narrow: with `-F t`, writing to standard output "is only allowed if the cluster has no additional tablespaces and WAL streaming is not used", per the [pg_basebackup reference](https://www.postgresql.org/docs/current/app-pgbasebackup.html).

### The protocol underneath it

`BASE_BACKUP` runs over the COPY protocol with a typed message framing, per the [streaming replication protocol](https://www.postgresql.org/docs/current/protocol-replication.html). The server sends two ordinary result sets first. The first holds the start position in `XLogRecPtr` format and the timeline ID. The second holds one row per tablespace with `spcoid`, `spclocation`, and an approximate `size` when progress was requested. One or more `CopyOutResponse` results follow, and each `CopyData` payload is one of four messages: `'n'` starts a new archive and gives its file name and source path, `'m'` starts the backup manifest, `'d'` holds archive or manifest data bytes, and `'p'` holds an Int64 count of bytes completed for the current tablespace. After the copy, a final ordinary result set contains the WAL end position.

The destination is a protocol option rather than a client concern. `TARGET 'client'` is the default and sends the data to the client, `TARGET 'server'` writes it to a server path named by `TARGET_DETAIL` and requires superuser or the `pg_write_server_files` role, and `TARGET 'blackhole'` discards it. `MANIFEST 'yes'` adds the manifest to the same stream, and `COMPRESSION` with `COMPRESSION_DETAIL` moves compression to the server side, supporting gzip, lz4, and zstd. `MAX_RATE` throttles the transfer in kilobytes per second, accepting zero or a value from 32 kB to 1 GB.

### Extending the target: basebackup_to_shell

`basebackup_to_shell` "adds a custom basebackup target called shell", so a caller runs `pg_basebackup --target=shell` or `--target=shell:DETAIL_STRING`, and "a server command chosen by the server administrator to be executed for each tar archive generated by the backup process. The command will receive the contents of the archive via standard input", per the [module documentation](https://www.postgresql.org/docs/current/basebackup-to-shell.html). `%f` in the command becomes the archive name, `%d` becomes the caller's target detail, and `basebackup_to_shell.required_role` limits who may use the target. PostgreSQL describes the module as "primarily intended as an example of how to create a new backup targets via an extension module", which makes the extension point, rather than the shell command, the precedent.

### XtraBackup to standard output, and onward to object storage

"Streaming mode sends a backup to STDOUT in the xbstream format instead of copying the files to the backup directory", invoked with `--stream`, per the [streaming backup documentation](https://docs.percona.com/percona-xtrabackup/8.4/take-streaming-backup.html). The stated reason is composition: "This method allows you to use other programs to filter the output of the backup, providing greater flexibility for storage of the backup." The documented example is `xtrabackup --backup --stream > backup.xbstream`.

The consumer at the far end of that pipe is the closest published precedent for issue #36's motivating case. The [xbcloud overview](https://docs.percona.com/percona-xtrabackup/8.4/xbcloud-binary-overview.html) describes it as "a cloud storage utility that works with Percona XtraBackup to upload, download, and manage database backups in cloud storage. It enables you to stream backups directly to cloud storage without requiring local disk space, making it ideal for large database environments". It offers three operations, `put`, `get`, and `delete`, and "accepts input via pipes from xbstream". It chunks the stream into separate objects named `backup_name/database/table.ibd.NNN...`, with a default chunk size of 10MB adjustable through `--read-buffer-size`, and retries failures with exponential backoff.

### ClickHouse: the destination named in the statement

ClickHouse inverts the shape, so the caller names the destination in SQL and the server performs the transfer. The general syntax is `BACKUP TABLE [db.]table_name TO S3('<S3 endpoint>/<path>', '<Access key ID>', '<Secret access key>', '<extra_credentials>')`, with `File('<path>/<filename>')`, `Disk('<disk_name>', '<path>/')`, and `AzureBlobStorage(...)` as the other destinations, per the [backup documentation](https://clickhouse.com/docs/operations/backup). `SETTINGS compression_method` and `compression_level` control compression, as in `BACKUP TABLE test.table TO Disk('backups', 'filename.zip') SETTINGS compression_method='lzma', compression_level=3`. `ASYNC` makes the command return immediately and run in the background, and without it "the backup process is synchronous and the command blocks until the backup completes". Named collections exist to keep credentials out of the statement and therefore out of the query log.

## What I could not establish

### Whether better-sqlite3 or node:sqlite can register a custom VFS from JavaScript

I read better-sqlite3's `docs/api.md` and Node's `sqlite` documentation page end to end and found no VFS entry point in either. I then fetched `src/better_sqlite3.cpp` from better-sqlite3, which turned out to be a 2 KB stub rather than the implementation, so I did not confirm at source level. Reading `src/objects/` and `src/util/` in better-sqlite3, and `src/node_sqlite.cc` in Node, would settle it.

### Whether VACUUM INTO to a URI target with `vfs=NAME` works end to end

The chain of evidence is strong, since `src/vacuum.c` ATTACHes the target filename and then parses a URI query parameter off it, and the URI documentation says ATTACH honours URI filenames when the connection was opened with them. I did not run the statement. A ten-line program registering a VFS and running `VACUUM INTO 'file:out.db?vfs=mine'` would settle it.

### How long a VACUUM INTO of a 170 GB SQLite database takes, and how much WAL accumulates in that window

No document I read gives a figure, and the WAL growth depends entirely on the write rate during the copy. Measuring on the deployment is the only way to get it.

### Whether the backup API's restart risk matters at this size

SQLite states the failure mode without a threshold: a backup "may never run to completion" if restarted often enough. Whether Sirannon's write rate crosses that line at 170 GB is a measurement rather than a lookup. The risk is avoidable in principle, because the restart rule applies only to writes from a different connection, and Sirannon already funnels writes through one writer connection.

### Whether sqlite3_rsync can run over a transport that is not ssh

The source shows `--origin` and `--replica` speaking the protocol on stdin and stdout, which any pipe satisfies, and `--ssh PATH` names "the SSH program used to reach the remote side" (usage text at line 37, parsed at line 2115 of `tool/sqlite3_rsync.c`). Whether that option accepts a command that is not an ssh client is unclear from the code I read, because lines 2268 to 2317 append ssh-shaped arguments around it. Running it against a substitute would settle it.

### The LTX file format

Litestream's how-it-works page describes what LTX files hold and how they compact, and it leaves out their byte layout. A format specification in the repository, if one exists, would settle it.

### Whether ClickHouse's `BACKUP ... TO S3(...)` stages locally first

The backup overview page does not say. Reading the ClickHouse backup implementation would settle it.

### Any figure for how much smaller a PostgreSQL incremental backup is than a full one

The documentation offers guidance without numbers, saying incremental backups "typically only make sense for relatively large databases where a significant portion of the data does not change, or only changes slowly".
