# A backup never blocks writes

A backup must copy the database while writes continue. The copy runs on the connection that writes, through SQLite's stepped backup interface, and it releases its lock between steps so that a checkpoint can fold the write-ahead log back into the main file in the gaps. Measured on a 1.05 GB database with 16,040 interleaved writes, the stepped copy held the log at 4,120,032 bytes, which is the 1000-frame automatic checkpoint threshold, while a single `VACUUM INTO` under one concurrent writer grew the log by roughly 11 MB per second for as long as the copy ran.

Sirannon runs those checkpoints itself rather than leaving them to SQLite, because incremental capture must read every log frame before a checkpoint allows SQLite to overwrite it. The prototype behind ADR 0006 measured 186 of 300 transactions lost when SQLite chose the checkpoint moments, and none lost when Sirannon captured first and checkpointed second.

The backup controller in the TypeScript implementation ran `VACUUM INTO` inside the writer lock before this decision, and the CDC and sync controllers share that lock, so a backup blocked user writes and replication apply for the whole copy. SQLite requires none of that, because `VACUUM INTO` takes only a read transaction on the source.

Every rule below follows from how SQLite behaves, so it binds every implementation. The measurements come from the TypeScript implementation's drivers, and they describe SQLite rather than those drivers.

## Why this over the alternatives

- **Keeping `VACUUM INTO` under the writer lock** blocks every write for the duration of the copy, which at the sizes this work targets would be measured in hours.
- **Holding a read transaction for the whole copy** produces a frozen snapshot and survives anything another connection does, but it pins the write-ahead log for the full duration, so the log grows by every write made during the copy. A bounded log and a frozen snapshot cannot be had together, and the restore path replays changes on top of the copy anyway, so the snapshot property buys nothing here.
- **Running the copy on a dedicated read connection** decouples the copy from the writer entirely, and it never finishes on a database taking writes. Section 2.2 of `docs/research/non-blocking-backup.md` measured one write on a second connection between every step: 4000 steps, 1943 pages still remaining, never completed. Both Node drivers reproduce it against an 8.6 MB database with a write every millisecond, at 141 restarts over 3001 steps on Node's own SQLite and 50 over 2001 on better-sqlite3, and neither run finished. Stepping on the writing connection instead completed the same copy in 132 and 133 steps with no restart, and both destinations passed `integrity_check`.

## Decision

- The copy runs on the connection that writes, through the stepped backup interface. A backup takes the writer lock to start its copy and releases it once the first step completes, because SQLite copies no pages and reports success when a transaction is already open on the source connection, while a transaction that opens once the copy is under way costs it nothing. The reference implementation measured ten steps running inside a write transaction opened mid-copy, with the copy completing and the destination passing `integrity_check`. The stepped interface is load-bearing rather than preferable, because a destination that accepts writes and returns nothing readable fails a `VACUUM INTO` at every size with `database disk image is malformed`, measured from 100 MB to 6.4 GB on both Node drivers. `VACUUM INTO` read its destination back 126 times during a 6.4 GB copy, where the stepped interface read 100 bytes once.
- Sirannon sets `wal_autocheckpoint = 0` on every database it opens with backups enabled, and runs its own cycle: capture the log frames, then checkpoint. The cycle interval sets the log bound directly, measured at 0.6 MB peak for a cycle every 25 transactions and 0.1 MB for every 5, against 6.7 MB and growing with no checkpointing at all.
- Sirannon counts a restart from a fall in the pages copied after a step, which is the total minus the remaining, because a restart returns the copy to page one. A rise in both counters is the source growing under the copy, which happens on every write through the copy's own connection and is not a restart. Sirannon stops after a small bound and reports an error that names what happened and what to do about it. A restart is never retried silently and never retried forever, because a copy that restarts endlessly at terabyte scale would burn disk and processor for months while reporting nothing.
- No Sirannon code path forces a `RESTART` or `TRUNCATE` checkpoint on a connection other than the copy's source while a copy runs, because a forcing checkpoint from a second connection sends an in-progress copy back to page one. Bulk load runs its `TRUNCATE` checkpoint on the writer connection, which is the copy's own source, and section 2.2 of the research measured that combination completing in 32 steps. An outside process forcing one remains a documented hazard, because Sirannon cannot prevent it.
- A copy advances one step per turn of the runtime's event loop, so a caller that never lets the loop reach the copy's continuation holds the copy still. The reference implementation measured two million writes and zero copy steps under a loop that yielded only to microtasks. Sirannon restarts a deadline on every step and fails the run when no step arrives inside it, so a held copy reports itself rather than hanging.
- A scheduled backup that finds the previous run still in progress skips, and the skip is reported with its reason. Queueing would build a permanent backlog behind a slow copy at exactly the sizes where backups matter most.

## Cost

The copy shares the connection that writes, so each step delays a queued write by the time that step takes. At the default of 256 pages a step moves 1 MiB of a 4 KiB-page database.

A backup is a consistent database that reflects writes made while the copy ran, and it is bound to no single instant. Restores land on chain-piece boundaries rather than arbitrary moments. The restart hard-stop means a machine where another process writes to the same file can fail its backups, and the error report is the mitigation.

Owning the checkpoint cycle changes behaviour beyond the backup path, because a database with backups enabled no longer checkpoints on SQLite's schedule. A Sirannon process that stops running its cycle while writes continue would let the log grow without bound, so the cycle must survive the failure of anything it depends on.

## References

- SQLite online backup API: <https://sqlite.org/backup.html>
- SQLite write-ahead logging, checkpoint starvation: <https://sqlite.org/wal.html>
- Measurements and source reading: `docs/research/non-blocking-backup.md`
- better-sqlite3 on the same rule: <https://github.com/WiseLibs/better-sqlite3/blob/master/docs/api.md>
- Mechanics of `VACUUM INTO` and the backup API: `docs/research/backup-interoperability.md`
