# Backups form a per-node chain captured from the write-ahead log

After the first full copy, each backup must capture only the write-ahead log frames written since the previous backup, so that the work and the storage of a backup scale with how much changed rather than with how large the database is. A 1 TB database that changed by 200 MB in a day pays for 200 MB.

A backup therefore stops being one file and becomes a chain: one full copy plus an ordered series of change pieces, where each piece records the chain position it starts from. This is the shape every established system converged on, and the industry evidence in `docs/research/backup-shapes-and-restore.md` shows the split is only over how visible the chain is to the operator.

## Why this over the alternatives

- **A full copy every cycle** fails at the sizes this work targets, because copying a terabyte nightly costs hours of I/O and a terabyte of transfer for a day of small changes.
- **A periodic scan that fingerprints fixed-size chunks and uploads the changed ones**, which is Verneuil's design, needs nothing new from the engine, but it reads the entire database every cycle, so the read cost scales with database size and breaks the rule this decision exists to satisfy.
- **Deriving increments from Sirannon's CDC change log** fails on coverage, because the log holds rows for watched tables only, retains one hour by default, and records nothing about indexes, unwatched tables, or storage state. The research file records why every production incremental scheme keys on a physical unit plus an always-increasing position instead.

## Decision

- Sirannon captures frames in-process on the node that owns the file, reading the documented log format directly. A prototype proved the reading: frame checksums validate against the values SQLite wrote, captured page bytes match the database pages they represent, commit markers align with transaction boundaries, and a base copy plus captured frames rebuilds a database that passes `integrity_check` with every row correct.
- Capture only works when Sirannon owns the checkpoint cycle, which ADR 0005 records. A checkpoint lets SQLite rewind the log and overwrite frames a reader has yet to take, and the loss is silent and permanent. Over 300 transactions, reading every 25 transactions while SQLite chose the checkpoint moments lost 186 of them, reading every 10 lost 6, and capturing before each checkpoint lost none. Reading after every single transaction also lost none, and Sirannon must not depend on that, because the margin closes as the write rate rises.
- Every capture compares the salt values in the log header against the ones it last saw. A checkpoint that rewinds the log changes those salts, so a reader tracking file offsets alone would read new frames as though they continued the old sequence. The prototype confirmed the rewind returns writing to offset 4,152 while such a reader would still expect offset 78,312, and the salt comparison is what turns that from silent corruption into a detected event.
- Every piece names the backup it depends on and the chain positions it covers. Sirannon persists those relationships and answers two questions directly: which pieces a restore to a given moment needs, and which pieces are safe to delete. PostgreSQL states in its own documentation that it has no such mechanism and leaves the tracking to the operator, and deleting the wrong piece there silently destroys the ability to restore. Sirannon closes that gap because it is the writer of the pieces and the tracking costs it little.
- Chains are per node. Replicas apply changes logically, so two nodes hold the same rows in physically different files, and a chain of physical pieces started on one node can never be continued from another. Percona Backup for MongoDB documents the same constraint and requires a fresh chain when its base node is lost.
- After a failover, the new preferred backup node starts a fresh chain with a full copy. Continuing the old chain across nodes would corrupt restores, so the fresh copy is the honest cost of logical replication.
- Completion and progress reports carry the chain position each piece was taken at, because without that position every subsequent backup would have to be a full copy.

## Cost

Restoring requires the full copy plus every piece after it, so Sirannon must also schedule periodic fresh full copies to bound chain length, and the storage holds one database plus the retained pieces. A failover costs one full copy at the worst possible time, which the operator can see coming from the completion reports.

Capture and checkpointing are now one cycle, so a fault in either half stops both: a capture that fails must stop the checkpoint that would follow it, or the next capture would find frames already overwritten. Sirannon must treat a stalled capture as a condition to report rather than one to skip past.

## References

- SQLite write-ahead log file format: <https://sqlite.org/walformat.html>
- Litestream's capture and compaction design: <https://litestream.io/how-it-works/>
- Industry evidence on chain shapes and who folds them back: `docs/research/backup-shapes-and-restore.md`
- Why a logical change log cannot supply increments: `docs/research/backup-interoperability.md`
