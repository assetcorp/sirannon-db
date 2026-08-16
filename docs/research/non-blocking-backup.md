# Non-blocking backup: the mechanisms, and what production systems do

[Issue #36](https://github.com/assetcorp/sirannon-db/issues/36) settled two requirements: a backup must never stop writes, and it must never let SQLite's write-ahead log grow without bound while a copy runs. This file establishes how those two requirements can be met and where they conflict, at database sizes up to a terabyte. It goes past [backup-interoperability.md](./backup-interoperability.md), which already established what the online backup API and VACUUM INTO each hold while they run, and that a long-running read stops SQLite folding the WAL back into the main file. Nothing here is a recommendation, and nothing here proposes a design.

Some of the questions below have no documented answer, so I settled them by running programs against SQLite and by reading the SQLite source. Every measurement in this file came from SQLite 3.53.4 (2026), built from [the amalgamation](https://sqlite.org/2026/sqlite-amalgamation-3530400.zip) with `SQLITE_ENABLE_SNAPSHOT` and `SQLITE_THREADSAFE=1`, compiled by Apple clang 17.0.0 on Darwin 25.5.0, arm64, APFS on internal NVMe. Source line numbers refer to [the matching source distribution](https://sqlite.org/2026/sqlite-src-3530400.zip). The test programs were written outside this repository, under `/tmp/sqlite-backup-research`, and are not part of the codebase.

## 1. What SQLite itself offers

### 1.1 The checkpoint, and what stops it

A checkpoint copies pages out of the WAL and back into the database file. The [WAL documentation](https://sqlite.org/wal.html) states the limit on it in section 2.2: "A checkpoint can run concurrently with readers, however the checkpoint must stop when it reaches a page in the WAL that is past the end mark of any current reader. The checkpoint has to stop at that point because otherwise it might overwrite part of the database file that the reader is actively using." The end mark belongs to the reader, and the same section defines it as "the location of the last valid commit record in the WAL" at the moment the read transaction started.

That rule is a single loop in `walCheckpoint` in `src/wal.c` (lines 2233 to 2251). The code sets `mxSafeFrame = pWal->hdr.mxFrame`, then iterates over the read-mark slots, calling `walBusyLock` on each slot whose value is below `mxSafeFrame`. When the lock succeeds, no reader is using that mark, so the checkpointer resets it. When the lock returns `SQLITE_BUSY`, a live reader holds it, and the code sets `mxSafeFrame = y`, which is that reader's mark. Backfilling then stops at the oldest live reader, and `pInfo->nBackfill` records how far it got (line 2337), so the next checkpoint resumes from there.

There are five checkpoint modes, and the [sqlite3_wal_checkpoint_v2 reference](https://sqlite.org/c3ref/wal_checkpoint_v2.html) distinguishes them by how much each one blocks. PASSIVE copies "as many frames as possible without waiting for any database readers or writers to finish", and "the busy-handler callback is never invoked in the SQLITE_CHECKPOINT_PASSIVE mode". FULL "blocks (it invokes the busy-handler callback) until there is no database writer and all readers are reading from the most recent database snapshot". RESTART adds a second wait, "until all readers are reading from the database file only. This ensures that the next writer will restart the log file from the beginning". TRUNCATE "works the same way as SQLITE_CHECKPOINT_RESTART with the addition that it also truncates the log file to zero bytes just prior to a successful return". NOOP "always checkpoints zero frames" and exists so a caller can read `*pnLog` and `*pnCkpt` without doing any work.

I measured the four modes that do work against a database with 600 frames in the WAL and one connection holding an open read transaction taken after the first 300 frames were written. The busy timeout on the checkpointing connection was 500 ms.

| Mode | Return code | Frames in the log | Frames checkpointed |
| --- | --- | --- | --- |
| PASSIVE | `SQLITE_OK` | 600 | 300 |
| FULL | `SQLITE_BUSY` | 600 | 300 |
| RESTART | `SQLITE_BUSY` | 600 | 300 |
| TRUNCATE | `SQLITE_BUSY` | 600 | 300 |
| TRUNCATE, after the reader ended | `SQLITE_OK` | 0 | 0 |

The three aggressive modes backfilled no more frames than PASSIVE did while the reader was open, and each one returned `SQLITE_BUSY` after invoking its busy handler, which is what the reference describes. The counts confirm the `mxSafeFrame` rule exactly: the 300 frames written before the reader started were backfilled, and the 300 written after it were not. Only the reader ending unblocked the rest.

### 1.2 The automatic checkpoint

Automatic checkpointing is on by default, and the [wal_autocheckpoint pragma documentation](https://sqlite.org/pragma.html#pragma_wal_autocheckpoint) gives both the threshold and the mode: "Autocheckpointing is enabled by default with an interval of 1000 or SQLITE_DEFAULT_WAL_AUTOCHECKPOINT", and "All automatic checkpoints are PASSIVE". `sqliteLimit.h` line 169 defines that constant as `1000`. Section 6 of the WAL documentation puts the same threshold in bytes: "new content is appended to the WAL file until the WAL file accumulates about 1000 pages (and is thus about 4MB in size) at which point a checkpoint is automatically run and the WAL file is recycled".

The implementation is `sqlite3WalDefaultHook` in `src/main.c` (line 2484), and it is worth reading because of what it discards. When the frame count reaches the threshold, the hook calls `sqlite3_wal_checkpoint(db, zDb)`, which is the PASSIVE entry point, wrapped in `sqlite3BeginBenignMalloc()`, and it returns `SQLITE_OK` whatever the checkpoint returned. So when a reader pins the log, the automatic checkpoint makes partial progress or none, reports nothing, and the WAL keeps growing. There is no error to catch and no counter to watch other than the file size itself, or `PRAGMA wal_checkpoint(NOOP)`, whose three returned columns are documented on the [wal_checkpoint pragma page](https://sqlite.org/pragma.html#pragma_wal_checkpoint) as the busy flag, "the number of modified pages that have been written to the write-ahead log file", and "the number of pages in the write-ahead log file that have been successfully moved back into the database file at the conclusion of the checkpoint".

`sqlite3_wal_hook` is the general form of the same interface. Section 3.1 of the WAL documentation states that "A program can also use sqlite3_wal_hook() to register a callback to be invoked whenever any transaction commits to the WAL. This callback can then invoke sqlite3_wal_checkpoint() or sqlite3_wal_checkpoint_v2() based on whatever criteria it thinks is appropriate. (The automatic checkpoint mechanism is implemented as a simple wrapper around sqlite3_wal_hook().)" A caller that registers its own hook therefore replaces the default threshold behaviour and can choose the mode, which is how a caller gains the option of running TRUNCATE instead of PASSIVE.

### 1.3 The size controls, and what they do not control

`PRAGMA journal_size_limit` is the only size setting SQLite offers for the WAL file, and it is a truncation rule rather than a cap. The [pragma documentation](https://sqlite.org/pragma.html#pragma_journal_size_limit) states its scope: "Each time a transaction is committed or a WAL file resets, SQLite compares the size of the rollback journal file or WAL file left in the file-system to the size limit set by this pragma and if the journal or WAL file is larger it is truncated to the limit."

The code narrows that further. In `sqlite3WalFrames` in `src/wal.c`, the truncation runs only under `if( isCommit && pWal->truncateOnCommit && pWal->mxWalSize>=0 )` at line 4220, and `truncateOnCommit` is set to 1 at line 4106, inside the block that writes a fresh WAL header. A WAL header is written when the log restarts, so the limit applies on the first commit into a restarted log. `sqlite3WalClose` applies it a second time, at line 2554, and only in persistent-WAL mode after a successful final checkpoint. Neither path runs while a reader pins the log, because a pinned log never resets. So `journal_size_limit` bounds the file left behind after a reset, and it bounds nothing while a copy is in progress.

The one mechanism that genuinely recycles the file is described in section 2.2 of the WAL documentation: "Whenever a write operation occurs, the writer checks how much progress the checkpointer has made, and if the entire WAL has been transferred into the database and synced and if no readers are making use of the WAL, then the writer will rewind the WAL back to the beginning and start putting new transactions at the beginning of the WAL. This mechanism prevents a WAL file from growing without bound." `walRestartLog` in `src/wal.c` (line 3875) is that check, and its conditions are strict: the writer must hold `readLock==0`, meaning the whole log has been backfilled and it is reading the database file directly, and `walLockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1)` must succeed, meaning no other reader holds any read mark. One live reader defeats both conditions.

Section 6 of the WAL documentation names this failure directly, under the heading "Checkpoint starvation": "if a database has many concurrent overlapping readers and there is always at least one active reader, then no checkpoints will be able to complete and hence the WAL file will grow without bound." Its own advice is to create gaps: "This scenario can be avoided by ensuring that there are 'reader gaps': times when no processes are reading from the database and that checkpoints are attempted during those times."

### 1.4 A stable read view that does not stop a checkpoint

SQLite has one interface for holding a database state that is older than the current one, the snapshot API, available when `SQLITE_ENABLE_SNAPSHOT` is compiled in. It does not solve this problem, for two reasons that the documentation states and that I confirmed by measurement.

The first is that a snapshot handle needs an open read transaction to be safe. The [sqlite3_snapshot_get reference](https://sqlite.org/c3ref/snapshot_get.html) states the guarantee and its boundary: "If a read-transaction is opened by this function, then it is guaranteed that the returned snapshot object may not be invalidated by a database writer or checkpointer until after the read-transaction is closed. This is not guaranteed if a read-transaction is already open when this function is called." A reader holding that transaction takes a read mark like any other reader, so the checkpointer stops at it. I measured this: with a snapshot reader open, a PASSIVE checkpoint backfilled 3 frames out of 753, and after that reader closed, an identical checkpoint backfilled all 753.

The second is that a snapshot cannot be re-opened once a checkpoint has passed it. The [sqlite3_snapshot_open reference](https://sqlite.org/c3ref/snapshot_open.html) says so plainly: "A call to sqlite3_snapshot_open() will fail to open if the specified snapshot has been overwritten by a checkpoint. In this case SQLITE_ERROR_SNAPSHOT is returned." The test in `walBeginReadTransaction` in `src/wal.c` (line 3447) is `pSnapshot->mxFrame>=pInfo->nBackfillAttempted` together with a salt comparison, and the comment above it lists the two ways a snapshot goes stale, a WAL reset and "A checkpoint as been attempted that wrote frames past pSnapshot->mxFrame into the database file. Note that the checkpoint need not have completed for this to cause problems." My measurement matched: re-opening a snapshot with no checkpoint in between returned `SQLITE_OK`, and re-opening the same snapshot after one PASSIVE checkpoint returned 769, which is `SQLITE_ERROR_SNAPSHOT`.

So the two properties are mutually exclusive by construction. A reader that holds a stable view holds a read mark, and a read mark is exactly what stops the checkpointer. `sqlite3WalSnapshotRecover` (line 3342) claws back a little ground by lowering `nBackfillAttempted` when the frames are still intact, and its own comment marks it as unsafe on some filesystems: "This is only really safe if the file-system is such that any page writes made by earlier checkpointers were atomic operations, which is not always true."

### 1.5 The stepped backup interface, and what happens in the gaps

The online backup API is the only copy mechanism SQLite offers that releases the source between units of work. Section 3.1 of the [backup documentation](https://sqlite.org/backup.html) describes the released state: "During the 250 ms sleep in step 3 above, no read-lock is held on the database file and the mutex associated with pDb is not held. This allows other threads to use database connection pDb and other connections to write to the underlying database file."

The code confirms that the read transaction belongs to the step rather than to the backup. In `sqlite3_backup_step` in `src/backup.c`, line 361 opens one only when none is open, `if( rc==SQLITE_OK && SQLITE_TXN_NONE==sqlite3BtreeTxnState(p->pSrc) )`, setting `bCloseTrans = 1`; lines 573 to 578 close it again at the end of the same call. The page-copy loop at line 415 runs between those two points.

The question the documentation leaves open is whether a checkpoint can complete in those gaps, and the answer is yes. My source database was 1,051,222,016 bytes, which is 256,646 pages of 4096 bytes. I copied it with the stepped API at 64 pages per step, running four single-row update transactions on the source connection after every step, 16,040 writes across 4010 steps, and sampled the WAL file after every step. The peak WAL size was 4,120,032 bytes on both runs, which is exactly 1000 frames plus the 32-byte header, the automatic checkpoint threshold. The two runs took 4.7 and 5.8 seconds. The log never grew past the threshold, so the automatic checkpoints that fired in the gaps between steps ran to completion, and they could do so because no reader held a mark at those moments.

The contrast is the snapshot-holding copy. I ran `VACUUM INTO` on the same database in one process while a second process committed single-row updates in a loop, and sampled the source WAL. Three runs took 2.681, 2.916, and 2.490 seconds, and the peak WAL reached 31,777,592, 31,007,152, and 29,791,752 bytes, which is between 7231 and 7712 frames against the 1000-frame threshold that was in force throughout. Crossing that threshold triggered the automatic checkpoint on every subsequent commit, and the file grew anyway, which is the silent failure section 1.2 above describes.

Two constraints on the caller follow from the same code. The first is that the source connection must not be inside a write transaction when a step runs. Line 350 of `src/backup.c` is `if( p->pDestDb && p->pSrc->pBt->inTransaction==TRANS_WRITE ){ rc = SQLITE_BUSY; }`, and I confirmed the value by opening `BEGIN IMMEDIATE` on the source connection and calling `sqlite3_backup_step`, which returned 5. That figure is worth noting because the [backup API reference](https://sqlite.org/c3ref/backup_finish.html) documents a different one: "If the source database connection is being used to write to the source database when sqlite3_backup_step() is called, then SQLITE_LOCKED is returned immediately." `SQLITE_LOCKED` is 6. Both are documented as retryable, so the practical effect is the same, and a caller that tests for `SQLITE_LOCKED` alone would miss the case in 3.53.4.

The second constraint is that the caller can choose to keep a read transaction open on the source connection, and doing so converts the stepped backup into a snapshot copy. Because line 361 only opens a transaction when none is open, an already-open read transaction survives every step, and the source pager therefore never re-reads the wal-index header. I measured the effect: with `BEGIN` plus a `SELECT` on the source connection, a copy completed while another connection wrote between every step, and completed again while another connection ran a TRUNCATE checkpoint between every step. Both of those cases fail without the held transaction, as section 2 shows. The price is the WAL, because the held read mark caps `mxSafeFrame` for the whole copy, and the price is also that the source connection cannot write, since ending its transaction to do so would end the snapshot.

### 1.6 What a terabyte-scale copy experiences

Two measured inputs drive the arithmetic. A single-row update of a 1000-byte value costs one WAL frame, and one frame at a 4096-byte page size is 4120 bytes; I measured 2000 such transactions producing a WAL of exactly 8,240,032 bytes with a reader pinning it, which is 2000 frames plus the header. The copy rate on this machine was 2.5 to 2.9 seconds per gigabyte for `VACUUM INTO` and 4.7 to 5.8 seconds per gigabyte for the stepped API with writes interleaved, in both cases with the source freshly written and therefore warm in the page cache, so both figures are optimistic for a cold terabyte.

Take the vacuum rate at face value and a 1 TB copy would run for about 46 minutes. At 100 single-page write transactions per second that is 276,000 frames, or 1.14 GB of WAL. At 1000 per second it is 11.4 GB. At 10,000 per second it is 114 GB. The fastest measured run produced 30.3 MB in 2.681 seconds, which is 11.3 MB per second from a single writer process looping as fast as it could, and that rate sustained for 46 minutes would be 31 GB. Every one of those figures is a lower bound on the real thing, because a cold 1 TB source would be slower to copy than a warm 1 GB one, and a longer copy accumulates proportionally more WAL.

The pinned WAL costs more than disk. Section 2.3 of the WAL documentation states the read cost: "read performance deteriorates as the WAL file grows in size since each reader must check the WAL file for the content and the time needed to check the WAL file is proportional to the size of the WAL file."

### 1.7 The wal2 branch

SQLite has a branch that solves the pinned-log problem directly, and it is not in any release. [The wal2 notes](https://sqlite.org/src/doc/wal2/doc/wal2.md) state the problem in the same terms as this file: "There are also circumstances in which long-running readers may prevent a checkpointer from checkpointing the entire wal file - also causing the wal file to grow indefinitely in a busy system. Wal2 mode does not have this problem. In wal2 mode, wal files do not grow indefinitely even if the checkpointer never has a chance to finish uninterrupted." The mechanism is two log files: "When data is written to the database, the writer begins by appending the new data to the first wal file. Once the first wal file has grown large enough, writers switch to appending data to the second wal file. At this point the first wal file can be checkpointed (after which it can be overwritten)."

The same page states the compatibility cost: "A database in wal2 mode may only be accessed by versions of SQLite compiled from this branch. Attempting to use any other version of SQLite results in an SQLITE_NOTADB error." I confirmed that the mode is absent from the release: `PRAGMA journal_mode=wal2` on 3.53.4 returned `delete`, leaving the mode unchanged. The notes do not say what happens when a reader pins the first log and the second one also fills, so the behaviour under a copy that outlasts both files is unestablished.

## 2. The restart problem

### 2.1 What triggers a restart

`sqlite3BackupRestart` in `src/backup.c` (line 728) is three lines long, and all it does is set `p->iNext = 1` for every attached backup, which returns the copy to page one. Its comment gives the reason: "This is called when the pager layer detects that the database has been modified by an external database connection. In this case there is no way of knowing which of the pages that have been copied into the destination database are still valid and which are not, so the entire process needs to be restarted."

The pager calls it from three places, and the one that matters is `pager_reset` at `src/pager.c` line 1783, which discards the page cache. `pager_reset` runs from `pagerBeginReadTransaction` (line 3308) whenever `sqlite3WalBeginReadTransaction` reports that the state changed, and from the shared-lock path (line 5465) in rollback-journal mode whenever the 16 bytes at offset 24 of the database file differ from the copy the connection last read.

Both detectors compare the current state against this connection's own cached copy of it, and that is the whole of the same-connection exemption. In WAL mode, a connection that writes updates its own `pWal->hdr` as it goes, so its next read transaction finds nothing changed. In rollback mode, `dbFileVers` is refreshed from page 1 whenever page 1 is read (`src/pager.c` line 3110). Writes made through the backup's own source connection take a different route entirely: `sqlite3BackupUpdate` in `src/backup.c` (line 713) copies the new page straight into the destination when `iPage<p->iNext`, so a page already copied is patched in place rather than invalidated.

The [backup API reference](https://sqlite.org/c3ref/backup_finish.html) states the rule at the level of connections: "If the source database is modified by an external process or via a database connection other than the one being used by the backup operation, then the backup will be automatically restarted by the next call to sqlite3_backup_step(). If the source database is modified by using the same database connection as is used by the backup operation, then the backup database is automatically updated at the same time."

### 2.2 What counts as another connection, measured

I copied an 8 MB database at 64 pages per step, ran one action between every step, and capped each run at 4000 steps. The database was 2007 pages, so an uninterrupted copy took 32 steps.

| Action taken between steps | Steps | Remaining pages | Outcome |
| --- | --- | --- | --- |
| Nothing | 32 | 0 | Completed |
| One write on the backup's own source connection | 32 | 0 | Completed |
| One write on a second connection in the same process | 4000 | 1943 | Never completed |
| A read transaction opened and closed on a second connection | 32 | 0 | Completed |
| `wal_checkpoint(PASSIVE)` on the source connection | 32 | 0 | Completed |
| `wal_checkpoint(PASSIVE)` on a second connection | 32 | 0 | Completed |
| `wal_checkpoint(TRUNCATE)` on the source connection | 32 | 0 | Completed |
| `wal_checkpoint(TRUNCATE)` on a second connection | 4000 | 1943 | Never completed |
| A write and then `wal_checkpoint(TRUNCATE)`, both on the source connection | 32 | 0 | Completed |

In both failing rows the copy never progressed past the 64 pages of a single step, across 4000 attempts. That is the behaviour section 3.1 of the [backup documentation](https://sqlite.org/backup.html) warns about without naming a threshold: "If the backup process is restarted frequently enough it may never run to completion and the backupDb() function may never return."

Two rows of that table are not in any documentation I read. A TRUNCATE checkpoint on a second connection restarts the copy even though it writes no user data, and a PASSIVE checkpoint on a second connection does not. The reason is in `walRestartHdr` in `src/wal.c` (line 2152), which TRUNCATE and RESTART reach through `walCheckpoint` line 2383 and PASSIVE never does. `walRestartHdr` increments `pWal->nCkpt`, sets `hdr.mxFrame` to 0, changes both salt values, and calls `walIndexWriteHdr`. That is a wal-index header change, so the source connection's next read transaction reports `changed` and the pager discards its cache. A PASSIVE checkpoint only advances `pInfo->nBackfill`, which is in `WalCkptInfo` rather than in `WalIndexHdr`, so no reader treats it as a change.

The practical form of the rule is therefore narrower than "another connection wrote". Any operation on any other connection that rewrites the wal-index header restarts the copy, and that includes the RESTART and TRUNCATE checkpoint modes, which are precisely the modes section 6 of the WAL documentation recommends for beating checkpoint starvation. The process boundary makes no difference either way; the connection is the unit.

### 2.3 The scale of the gap a copy needs

A restart discards all progress, so a stepped copy completes only when the whole database is copied inside one gap between foreign writes. On the measured rate of 4.7 to 5.8 seconds per gigabyte, a 1 TB database would need a gap of 80 to 99 minutes with no write and no aggressive checkpoint on any other connection. A deployment that cannot promise that gap cannot use the plain stepped copy in the presence of other writing connections, whatever the step size.

### 2.4 Published guidance

There is little. Across the whole SQLite 3.53.4 documentation set, three files mention the restart at all, and they carry the same two paragraphs: `backup.html`, `c3ref/backup_finish.html`, and `capi3ref.html`. Section 3.1 of `backup.html` adds one qualification the C reference omits: the exception applies only when "the source database is not an in-memory database, and the write is performed from within the same process as the backup operation and uses the same database handle (pDb)", and it confirms the outcome of a completed copy, "the user can be sure that when the backup operation is completed the backup database contains a consistent and up-to-date snapshot of the original".

The clearest published advice comes from a driver rather than from SQLite. better-sqlite3's [API documentation](https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md) states it as a deployment rule: "it's recommended that only a single connection is responsible for mutating the database if online backups are being performed."

### 2.5 Where Sirannon stands against that rule

`ConnectionPool` in `packages/ts/src/core/connection-pool.ts` opens one writer and a set of readers (lines 37 to 78), and `acquireWriter` returns that single writer (line 96). A backup that took its source connection from `acquireWriter` would therefore be exempt from restarts caused by Sirannon's own writes, and would remain exposed to any other process holding the same file open. One internal operation would break the exemption if it ever ran on a different connection: `checkpoint` in `packages/ts/src/core/bulk-load.ts` (line 117) runs `PRAGMA wal_checkpoint(TRUNCATE)`, and today it takes the writer connection passed in as `run.writer`.

## 3. What production systems do

Every SQLite product below falls into one of two groups. One group holds a long read transaction, accepts that the checkpointer stops, and then manages the consequence. The other group never holds one, because it captures changes somewhere other than through a reading connection.

### 3.1 Litestream holds the reader and keeps an emergency brake

Litestream takes the checkpointer away from SQLite and runs it itself. Its [how-it-works page](https://litestream.io/how-it-works/) states the approach: "It starts a long-running read transaction to prevent any other process from checkpointing and restarting the WAL file. Instead, it continually reads new WAL pages and manually calls out to SQLite to perform checkpoints as necessary."

Both halves are in `db.go` at commit `97d2dfa513a37dd6ac0e1521962a88d479be5e07`. Line 1046 opens the connection with `_pragma=wal_autocheckpoint(0)` in the DSN, which disables SQLite's own threshold. `acquireReadLock` (line 1183) opens a transaction and runs `SELECT COUNT(1) FROM _litestream_seq;` to materialise the read lock, under the comment "Start long running read-transaction to prevent checkpoints".

The interesting part is what Litestream does around its own checkpoints. `execCheckpoint` (line 2653) drops the read lock first and takes it back afterwards: "Ensure the read lock has been removed before issuing a checkpoint. We defer the re-acquire to ensure it occurs even on an early return." Litestream manufactures the reader gap that section 6 of the WAL documentation asks for.

The policy is written out in the type comment at `db.go` line 50, with the defaults at lines 33 to 45. There are three tiers: a PASSIVE checkpoint at `DefaultMinCheckpointPageN = 1000` pages, "Non-blocking checkpoint at ~1k pages (~4MB)"; a PASSIVE checkpoint on a timer, `DefaultCheckpointInterval = 1 * time.Minute`; and a TRUNCATE checkpoint at `DefaultTruncatePageN = 121359`, annotated `// ~500MB with 4KB page size` and described as an "Emergency brake for runaway WAL growth. Can block writes while waiting for long-lived read transactions." A fourth mode was removed: "The RESTART checkpoint mode was permanently removed due to production issues with indefinite write blocking (issue #724)."

Litestream's own [WAL truncate threshold guide](https://litestream.io/guides/wal-truncate-threshold/) is unusually direct about the residual risk. On the brake: "A genuine blocking TRUNCATE runs only when the passive checkpoint does not bring the WAL back below the threshold, for example, when a long-lived reader keeps the WAL open so it cannot be restarted. When a TRUNCATE is forced, it always takes a full boundary snapshot while holding the write lock, which can stall writers on a large database." On what the threshold buys: "truncate-page-n bounds WAL growth rather than guaranteeing the -wal file shrinks: disk usage stays roughly bounded to the threshold plus catch-up overshoot." And on turning the brake off, which the guide recommends for applications with long reads: "Disadvantages: WAL can grow unbounded if read transactions prevent checkpointing. Requires disk space monitoring and alerting."

Litestream therefore presents the choice as a straight trade, in its own configuration guide: either writes stall occasionally, or the log has no bound.

### 3.2 sqlite3_rsync holds the reader and publishes nothing about the log

The [sqlite3_rsync documentation](https://sqlite.org/rsync.html) promises both properties this issue asks for on the origin side: "REPLICA becomes a copy of a snapshot of ORIGIN as it existed when the sqlite3_rsync command started", and "Other programs can write to ORIGIN and can read from REPLICA while this utility runs." The snapshot comes from a plain read transaction, opened by `originSide` in `tool/sqlite3_rsync.c` with `runSql(p, "BEGIN")` at line 1393 and held until the transfer ends.

I grepped the whole tool for `wal_checkpoint`, `wal_autocheckpoint`, and any size threshold, and found none: the only WAL-related statement in the file is a `PRAGMA journal_mode` read at line 1395. The origin's WAL therefore grows for the whole run, exactly as it does under `VACUUM INTO`, and neither the documentation nor the source mentions it.

### 3.3 rqlite takes the checkpointer away from SQLite entirely

rqlite disables automatic checkpointing at `db/db.go` line 294 with `PRAGMA wal_autocheckpoint=0`, and the line above it explains the constraint that forces its connection pooling: `rwDB.SetMaxOpenConns(1) // Key to ensure a new connection doesn't enable checkpointing`. Its `db/DESIGN.md` spells out why one connection matters: "That PRAGMA is per-connection. If `database/sql` were free to open additional read/write connections from its pool, each fresh connection would arrive with `wal_autocheckpoint=1000` re-enabled, and SQLite would silently start checkpointing behind the snapshot subsystem's back."

The bound comes from the Raft snapshot, triggered on WAL size. `cmd/rqlited/flags.toml` defines `RaftSnapThresholdWALSize` with `default = 4194304` and this help text: "rqlite, by default, will also trigger a snapshot if the WAL gets larger than 4MB. Large SQLite WAL files can decrease query performance, and since snapshotting involves checkpointing the WAL file, snapshotting is an effective way to limit WAL size. However writes are blocked during the snapshotting process, so it's a trade-off."

rqlite also had the failure this file is about, and fixed it. `db/DESIGN.md` records the history: "Prior to v10, checkpointing was effectively 'wait forever for readers to release, then truncate.' A persistently-blocked reader could stall checkpointing indefinitely, which would in turn stall Raft's log truncation and bring write throughput to a halt across the cluster." The replacement is a checkpoint manager that treats a blocked checkpoint as a partial success and resumes: "Each checkpoint either truncates, makes partial progress that the next attempt continues from, or returns retryable-busy without touching state."

### 3.4 libSQL writes a second log so that replicas never hold a reader

libSQL replaces SQLite's WAL implementation with a vtable and writes every frame twice, once into the SQLite WAL and once into an independent file named `wallog`, created in `ReplicationLogger::open` in `libsql-server/src/replication/primary/logger.rs`. Replicas stream from `wallog` and from snapshot files rather than from a read transaction, so a replica that falls hours behind pins nothing.

The two logs are bounded separately. The SQLite WAL keeps SQLite's own threshold, `const DEFAULT_AUTO_CHECKPOINT: u32 = 1000` at `libsql-server/src/lib.rs` line 97, with an alternative time-based checkpoint that runs `PRAGMA wal_checkpoint(TRUNCATE)` and retries after `RETRY_INTERVAL = Duration::from_secs(60)` on failure. The replication log is bounded by `max_log_size`, default 200 MB, and `LogFile::should_compact` at `logger.rs` line 344 compacts when `self.header.frame_count.get() > self.max_log_frame_count`. A replica that has fallen past the compaction point receives `LogReadError::SnapshotRequired` and re-syncs from a snapshot, so the log is never held open for a slow consumer.

Turso's newer sync engine states the same problem from the client side. Its [checkpoint documentation](https://docs.turso.tech/sync/checkpoint) says "Auto-checkpoint is disabled for sync databases, you must call checkpoint() explicitly", and "Without checkpointing, the WAL grows unbounded. After many writes, the WAL can become significantly larger than the database itself."

### 3.5 dqlite replicates page sets and never copies a live file

dqlite's VFS holds the database and the WAL as in-memory images, and a commit becomes a Raft log entry carrying the modified pages, per [its replication documentation](https://canonical.com/dqlite/docs/explanation/replication). It disables SQLite's checkpointer at `src/vfs.c` line 2564 with `sqlite3_wal_autocheckpoint(db, 0)`, and checks a frame count after every applied transaction at line 2898, against `DEFAULT_CHECKPOINT_THRESHOLD` of 1000 defined in `src/config.c` line 24. Its checkpoint takes every shared-memory lock exclusively first, so a reader in progress makes the attempt fail, and the next applied transaction retries.

### 3.6 Expensify Bedrock runs the wal2 branch

Bedrock takes the route section 1.7 describes. `sqlitecluster/SQLite.cpp` line 284 runs `PRAGMA journal_mode = wal2;` on every connection, and line 314 registers a WAL hook, which disables automatic checkpointing, under the comment "Setting a wal hook prevents auto-checkpointing". Its checkpoint mode is a command-line option defaulting to the non-blocking one, `SETDEFAULT("-checkpointMode", "PASSIVE")` at `main.cpp` line 337. A reader therefore causes a partial checkpoint and no blocking, and the alternation between the two log files is what keeps growth bounded.

### 3.7 The two products with no answer

LiteFS captures each transaction from the filesystem layer rather than through a connection, so ordinary replication holds no reader. Its full `Export` path is the exception, because it takes the checkpoint and read locks for the length of the transfer. Whether anything bounds the log during that transfer is unestablished: the search behind this paragraph found no threshold and no `wal_autocheckpoint` call in the LiteFS source, and I did not open that source myself, so treat the conclusion as unconfirmed. Cloudflare's D1 and Durable Objects publish a recovery interface over a 30 day window in the [SQLite storage API documentation](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/) and nothing about how a copy is taken, and the storage backend is absent from the open-source `workerd` tree.

### 3.8 PostgreSQL: the log is unbounded by default, and the operator chooses the failure

PostgreSQL takes a torn file copy and repairs it by replaying WAL, so writes never block and no snapshot is held. The [continuous archiving documentation](https://www.postgresql.org/docs/17/continuous-archiving.html) states both halves: "It is neither necessary nor desirable to stop normal operation of the database while you do this", and "It is not necessary to be concerned about the amount of time it takes to make a base backup."

What holds the WAL is a replication slot, and `pg_basebackup` creates one by default. The [pg_basebackup reference](https://www.postgresql.org/docs/17/app-pgbasebackup.html) documents this under `--no-slot`: "By default, if log streaming is selected but no slot name is given with the -S option, then a temporary replication slot is created (if supported by the source server) ... Using a replication slot is almost always preferred, because it prevents needed WAL from being removed by the server during the backup."

The bound on that retention is off by default. The [replication configuration documentation](https://www.postgresql.org/docs/17/runtime-config-replication.html) states it exactly: "Specify the maximum size of WAL files that replication slots are allowed to retain in the pg_wal directory at checkpoint time. If max_slot_wal_keep_size is -1 (the default), replication slots may retain an unlimited amount of WAL files." Setting it turns unbounded growth into a failed backup, since the server invalidates the slot at the next checkpoint. Leaving it unset keeps the backup and risks the disk, and the same documentation names that outcome: "If the file system containing pg_wal/ fills up, PostgreSQL will do a PANIC shutdown."

### 3.9 MySQL with InnoDB: the log is bounded, so the backup is what breaks

InnoDB inverts the problem, because its redo log has a fixed capacity and is written in a circle. Percona XtraBackup copies the data files while writes continue and tails the redo log alongside, and [its own description](https://docs.percona.com/percona-xtrabackup/8.4/how-xtrabackup-works.html) names the risk: "Percona XtraBackup remembers the LSN when it starts, and then copies the data files. The operation takes time, and the files may change ... Percona XtraBackup also runs a background process that watches the transaction log files, and copies any changes. Percona XtraBackup does this continually. The transaction logs are written in a round-robin fashion, and can be reused."

The capacity is `innodb_redo_log_capacity`, and the default in `storage/innobase/handler/ha_innodb.cc` at tag `mysql-8.4.0` is `100 * 1024 * 1024`, so 100 MB. A backup that falls behind that window loses records it needed. XtraBackup's answer is an option that is off by default, documented in [its option reference](https://docs.percona.com/percona-xtrabackup/8.4/xtrabackup-option-reference.html): "register-redo-log-consumer ... This option is disabled by default. When enabled, this options lets Percona XtraBackup register as a redo log consumer at the start of the backup. The server does not remove a redo log that Percona XtraBackup (the consumer) has not yet copied ... The server blocks the writes during the process."

That sentence is the same trade as Litestream's emergency brake and PostgreSQL's slot, stated in one line. Either the log is bounded and the backup can fail, or the backup is guaranteed and the server stalls writes or fills the disk.

SQL Server makes the third choice, which is to let the log grow and fail the database rather than the backup. Its [transaction log documentation](https://learn.microsoft.com/en-us/sql/relational-databases/logs/the-transaction-log-sql-server) lists `ACTIVE_BACKUP_OR_RESTORE` as reason 3 for a delayed truncation, "A data backup or a restore is in progress. (All recovery models.) If a data backup is preventing log truncation, canceling the backup operation might help the immediate problem", and states the endpoint on the same page: "If a transaction log is never truncated, it eventually fills all the disk space allocated to physical log files."

## 4. What bounds the log

Only one thing returns a SQLite WAL to the start of the file, and that is a log reset. Section 2.2 of the [WAL documentation](https://sqlite.org/wal.html) describes it, and `walRestartLog` in `src/wal.c` (line 3875) enforces two conditions before it happens: the writer must hold `readLock==0`, which means the entire log has already been backfilled into the database file, and `walLockExclusive(pWal, WAL_READ_LOCK(1), WAL_NREADER-1)` must succeed, which means no other connection holds any read mark. A single live reader defeats both. Every other control acts on the reset rather than on growth.

| Mechanism | Does it bound the WAL while a copy runs? |
| --- | --- |
| `wal_autocheckpoint`, default 1000 frames | Yes when no reader pins the log, and silently not at all when one does. Measured: a 4,120,032-byte peak across a stepped copy of a 1.05 GB database, against 31,777,592 bytes across a `VACUUM INTO` of the same file |
| `PRAGMA journal_size_limit` | No. It truncates the file on the first commit after a reset, and a pinned log never resets |
| A FULL, RESTART, or TRUNCATE checkpoint | No. Each one waits on the busy handler and then returns `SQLITE_BUSY`, having backfilled no more than PASSIVE would. Measured in section 1.1 |
| The snapshot API | No. A snapshot needs an open read transaction to stay valid, and that transaction is itself the thing stopping the checkpointer |
| Reader gaps between stepped copy units | Yes, and this is the only mechanism in released SQLite that keeps a long copy and a bounded log at the same time |
| `wal2` mode | Yes, by alternating two log files, and it is on a branch rather than in a release |

The arithmetic is straightforward once the growth rate is known. I measured one single-row update transaction against a 1000-byte value costing exactly one frame, and one frame at a 4096-byte page size is 4120 bytes: 2000 such transactions with a reader holding the log produced a WAL of 8,240,032 bytes, which is 2000 frames plus the 32-byte header. Multi-page transactions cost one frame per distinct page, so the general form is transactions per second, times pages touched per transaction, times 4120 bytes, times the duration of the copy.

| Write rate | WAL after 10 minutes | After 46 minutes | After 4 hours |
| --- | --- | --- | --- |
| 100 single-page transactions per second | 247 MB | 1.14 GB | 5.93 GB |
| 1000 per second | 2.47 GB | 11.4 GB | 59.3 GB |
| 10,000 per second | 24.7 GB | 114 GB | 593 GB |

Those rows assume the copy pins the log for its whole duration. The 46 minute column is there because 46 minutes is what a 1 TB copy would take at the rate I measured for `VACUUM INTO`, 2.5 to 2.9 seconds per gigabyte, on a warm source. A cold terabyte would take longer, so every figure in that column is a lower bound.

## What is open, what is closed, and what is unestablished

### Routes that are genuinely open

**Stepping the backup API from the connection that also writes, with no read transaction held.** This is the only combination in released SQLite that satisfies both requirements at once. Writes are never blocked for the length of the copy, because the writer and the copy interleave on one connection and a step is short; I measured 32 steps of 64 pages over a 2007-page database in 11 ms. The log stays bounded because every gap between steps is a reader gap, which is what lets the automatic checkpoint complete; I measured a peak WAL of 4,120,032 bytes across a copy of a 1.05 GB database with 16,040 interleaved writes, which is the 1000-frame threshold to the byte. Its conditions are strict, and section 2.2 lists them: no other connection may write to the source, and no other connection may run a RESTART or TRUNCATE checkpoint, for the whole duration.

**Stepping the backup API with a read transaction held open on the source connection.** This buys immunity from both failure modes above, and I measured copies completing while another connection wrote between every step and while another connection ran TRUNCATE checkpoints between every step. It also buys a true point-in-time snapshot. It costs the WAL for the full duration of the copy, at the rates in the table above, and it stops the source connection writing until the copy ends.

**Capturing changes somewhere other than a reading connection.** libSQL writes a second log and serves replicas from it, and dqlite replicates page sets through Raft from an in-memory VFS, so neither one holds a read mark on behalf of a consumer. Neither project publishes anything about a copy pinning the log, because in neither design does a copy hold a reader. This is a different shape of system rather than a different backup call.

**Accepting a bounded log with an occasional write stall.** Litestream's blocking TRUNCATE at roughly 500 MB, PostgreSQL's `max_slot_wal_keep_size`, and XtraBackup's `--register-redo-log-consumer` are the same decision in three products, though they differ in what they protect: Litestream and XtraBackup stall writes to keep the bound, and PostgreSQL breaks the backup instead. Litestream's brake is on by default, and the other two are off. This route contradicts the "never stop writes" requirement in issue #36, so it is open only if that requirement is softened.

### Dead ends, and why

**`PRAGMA journal_size_limit` as a cap.** It acts on a reset, and a pinned log never resets. The code path is `src/wal.c` line 4220, gated on `truncateOnCommit`, which is set only when a fresh WAL header is written.

**The snapshot API as a way to read stably without stopping the checkpointer.** The two properties are mutually exclusive in the implementation. A valid snapshot needs its read transaction held, and I measured a snapshot reader limiting a checkpoint to 3 frames out of 753. Releasing the transaction and re-opening the snapshot later fails with `SQLITE_ERROR_SNAPSHOT` once any checkpoint has passed it, which I measured after a single PASSIVE checkpoint.

**Running an aggressive checkpointer on a separate connection during a stepped copy.** A TRUNCATE or RESTART checkpoint on any connection other than the copy's source restarts the copy from page one, because `walRestartHdr` rewrites the wal-index header. I measured a copy failing to progress past 64 pages across 4000 steps under exactly that pattern. This is not in any SQLite documentation, and it removes the obvious way of fighting checkpoint starvation while a copy runs.

**`VACUUM INTO` or `sqlite3_rsync` for a terabyte with concurrent writes.** Both hold one read transaction for the whole copy, so both pin the log for the whole copy, and neither offers any control over it. The measured growth was up to 31,777,592 bytes in 2.681 seconds against a single looping writer.

**A stepped copy in a deployment where a second connection writes.** The livelock is total rather than gradual: 4000 steps, no progress. For the copy to complete, the whole database must be copied inside one gap between foreign writes, which at the measured rate would be 80 to 99 minutes of quiet for a terabyte.

**`wal2` today.** The mode solves the problem by design, and `PRAGMA journal_mode=wal2` on 3.53.4 returns `delete`, leaving the database unchanged. A database in that mode is unreadable by any released SQLite, which forces the branch on every reader of the file.

### What remains unestablished

**What wal2 does when both logs are pinned.** The notes state that the files do not grow indefinitely and do not say what happens when a reader holds the first log while the second one fills. Reading the branch's `wal.c` would settle it.

**The copy rate for a cold terabyte on the target hardware.** Every duration in this file came from a 1.05 GB source that had been written moments earlier and was therefore in the page cache, on an Apple-silicon NVMe. The 46 minute figure for 1 TB is arithmetic from that, and it is optimistic. Measuring on the deployment is the only way to get a real one.

**The cost of `backupUpdate` at scale.** Every write to a page below the copy's cursor costs an extra write into the destination, so a long copy under a heavy write load pays a second write for a share of the workload. I read the code path at `src/backup.c` line 713 and did not measure the overhead.

**Whether the `SQLITE_BUSY` return in `sqlite3_backup_step` is intentional.** The code sets it at `src/backup.c` line 350 and the documentation promises `SQLITE_LOCKED`. I confirmed the behaviour by running it, and I did not check the change history or ask the SQLite project which one is wrong.

**LiteFS during a full export.** That the log is unbounded is an inference from the locks the export takes and from a reported absence of any threshold in the source, and the project states nothing on the point. I did not open the LiteFS source myself, so this is the one product claim in section 3 that I have not checked at first hand.

**Cloudflare D1 and Durable Objects.** A search of Cloudflare's documentation and of the open-source `workerd` tree turned up nothing on how a consistent copy is taken or how any log is bounded, and the storage backend is not in that tree.

**Whether every Sirannon code path respects the single-writer-connection rule.** I read `ConnectionPool` and the one checkpoint call site in `bulk-load.ts`. I did not trace every statement path, nor any driver that opens its own connection, nor what a second process opening the same file would do.
