# A backup never blocks writes

A backup must copy the database while writes continue. The copy runs on its own read connection through SQLite's stepped backup interface, and it releases its lock between steps so that SQLite can checkpoint the write-ahead log in the gaps. Measured on a 1.05 GB database with 16,040 interleaved writes, the stepped copy held the log at 4,120,032 bytes, which is the 1000-frame automatic checkpoint threshold, while a single `VACUUM INTO` under one concurrent writer grew the log by roughly 11 MB per second for as long as the copy ran.

Today the backup controller runs `VACUUM INTO` inside the writer lock, and the CDC and sync controllers share that lock, so a backup blocks user writes and replication apply for the whole copy. SQLite requires none of that, because `VACUUM INTO` takes only a read transaction on the source.

## Why this over the alternatives

- **Keeping `VACUUM INTO` under the writer lock** blocks every write for the duration of the copy, which at the sizes this work targets would be measured in hours.
- **Holding a read transaction for the whole copy** produces a frozen snapshot and survives anything another connection does, but it pins the write-ahead log for the full duration, so the log grows by every write made during the copy. A bounded log and a frozen snapshot cannot be had together, and the restore path replays changes on top of the copy anyway, so the snapshot property buys nothing here.
- **Running the copy on the writer connection** avoids the restart rule, because only writes from a different connection restart a stepped copy, but it re-couples the copy to ordinary writes, which is the coupling this decision removes.

## Decision

- The copy runs on a dedicated read connection through the stepped backup interface, and no backup operation takes the writer lock.
- Sirannon watches the pages-remaining counter after every step. A jump upwards means another connection restarted the copy. Sirannon counts restarts, stops after a small bound, and reports an error that names what happened and what to do about it. A restart is never retried silently and never retried forever, because a copy that restarts endlessly at terabyte scale would burn disk and processor for months while reporting nothing.
- Bulk load no longer forces a `TRUNCATE` checkpoint, because a forcing checkpoint from a second connection sends an in-progress copy back to page one, and bulk load is the one such trigger inside Sirannon. An outside process forcing a `RESTART` or `TRUNCATE` checkpoint remains a documented hazard, because Sirannon cannot prevent it.
- A scheduled backup that finds the previous run still in progress skips, and the skip is reported with its reason. Queueing would build a permanent backlog behind a slow copy at exactly the sizes where backups matter most.

## Cost

A backup is a consistent database that reflects writes made while the copy ran, and it is bound to no single instant. Restores land on chain-piece boundaries rather than arbitrary moments. The restart hard-stop means a machine under heavy foreign-connection churn can fail its backups, and the error report is the mitigation.

## References

- SQLite online backup API: <https://sqlite.org/backup.html>
- SQLite write-ahead logging, checkpoint starvation: <https://sqlite.org/wal.html>
- Measurements and source reading: `docs/research/non-blocking-backup.md`
- Mechanics of `VACUUM INTO` and the backup API: `docs/research/backup-interoperability.md`
