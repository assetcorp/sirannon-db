# Sirannon domain vocabulary

This file holds the terms this repository uses with a fixed meaning, so a conversation or a document can use them without redefining them. The decision records in `docs/adr/` hold the reasoning behind the designs these terms come from, and `packages/spec/` holds the contract every implementation follows. A term missing here is a term worth adding when it earns a second use.

## Core

- A **driver** adapts one SQLite runtime, such as better-sqlite3 or Node's built-in SQLite, to the contract in `core/driver/types.ts`, and it declares which optional capabilities it provides.
- The **writer lock** serialises every write to one database, and the CDC and sync controllers run their work under the same lock.
- The **change log** is the `_sirannon_changes` table, where triggers record row-level inserts, updates, and deletes for watched tables. It serves subscriptions and replication, and it retains one hour by default.
- The **write-ahead log**, or WAL, is the file SQLite appends committed changes to before folding them back into the main database file. The fold-back is a **checkpoint**, and a long-lived reader stops checkpoints from making progress.

## Backup

- A **backup chain** is one full copy of a database plus an ordered series of change pieces. The newest piece alone restores nothing, and the chain restores everything up to its end.
- A **full copy** is a complete, openable SQLite file produced by the stepped backup interface on a dedicated read connection, so writes continue while it runs.
- A **change piece** captures the write-ahead log frames written since the previous backup, so its size scales with what changed rather than with the size of the database.
- The **chain position** is the always-increasing marker recording where in the database's change sequence a piece starts and ends. Every completion report carries it, and the next change piece starts from it.
- A **destination** is the caller-supplied pair of directions Sirannon moves backup bytes through: write a piece, read a piece, and list pieces. Pieces are fixed in size and arrive in any order, because SQLite writes page one last. Sirannon carries no storage client of its own.
- The **checkpoint cycle** is Sirannon capturing the log frames and then folding the log back into the main file, in that order. SQLite's automatic checkpointing is off wherever backups are on, because a checkpoint Sirannon did not choose overwrites frames it has yet to capture.
- The **streaming extension** is the compiled SQLite VFS Sirannon owns, published as one optional npm package per platform, which delivers a full copy to a destination without writing a local file. Where no binary exists for a platform, the **staged fallback** writes a temporary local file first and declares that it did.
- A **capability report** is how a runtime states which backup operations it supports, so a caller learns before running that a browser hands over whole databases only.
- A **restart** is the stepped copy returning to page one because another connection wrote to the source or forced a checkpoint. Sirannon counts restarts and stops with an error after a bound, and it never retries silently.
- The **preferred backup node** is the one node in a replication group whose scheduled backup proceeds, chosen by the coordinator, replica-preferred by default. Chains belong to the node that started them, so a failover starts a fresh chain.
- A **restore** names a moment and produces a database. Sirannon selects the pieces, fetches them through the destination one at a time, and composes the file as they arrive, with a disk floor of the finished database plus one piece plus a capped change log.
- **Safe-to-delete** is the question Sirannon answers from its chain records: which pieces no possible restore still needs.
