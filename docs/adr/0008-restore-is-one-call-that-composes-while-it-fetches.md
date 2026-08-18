# Restore is one call that composes while it fetches

A caller must restore by naming a moment and receiving a database. Sirannon reads the chain records, selects the full copy and the change pieces the moment requires, fetches them through the destination interface, and composes them into an ordinary SQLite file as they arrive. The chain stays open to inspection for anyone who wants to look at the pieces, and nobody has to understand it to restore.

## Why this over the alternatives

- **A separate combine step the operator runs first**, which is PostgreSQL's shape, adds a second command to get right on the worst day, and PostgreSQL's tool reads only a local filesystem, so its answer to pieces in object storage is to download everything first. The industry evidence shows the tools people run against object storage, pgBackRest and WAL-G, compose during the fetch instead, and SQL Server folds the whole step into `RESTORE`.
- **Downloading the whole chain and then merging** needs free disk for the chain plus the output at once, which at terabyte scale is more than twice the database, and PostgreSQL's incremental restore documents exactly that cost.

## Decision

- Restore fetches pieces through the same caller-supplied interface that wrote them, holds one piece at a time, and applies it before fetching the next.
- The disk floor for a restore is the size of the finished database, plus one piece, plus a change log that Sirannon caps by applying pieces in batched transactions. Piece size and batch size are settings with defaults rather than constants. Sirannon publishes this arithmetic in its documentation, so an operator can compute their own floor before they need it. Any design in which the working space scales with the size of the database is rejected, which is what applying the whole restore as one transaction would do.
- Producing a database needs room for a database, and no established system escapes that floor, so the no-spare-disk rule this work set for backups relaxes to exactly the floor above for restores.
- The server exposes restore behind an explicit opt-in flag that is off by default, the same pattern as `acceptSql`, because an in-place restore over the network destroys the running database, and no default configuration should reach a destructive route. Elasticsearch's snapshot API is the precedent for restore over HTTP, and it ships authenticated.
- Serving read-only queries straight from a backup in remote storage, which Litestream's VFS does with on-demand page fetches, stays out of this change and stays possible, because the two-direction destination interface is the only dependency it would need. It is a read path with its own questions about staleness and caching, and folding it in would weaken both designs.

## Cost

Sirannon grows a restore path, which it has never had, with the correctness burden that carries: a restore must verify each piece against its recorded fingerprint and refuse a chain with a gap. The one-call shape means Sirannon owns piece selection, and a defect there fails restores rather than backups, which is the worse place to fail. The verification and the safe-to-delete records from the chain decision are the mitigations.

## References

- pg_combinebackup and its local-filesystem limit: <https://www.postgresql.org/docs/current/app-pgcombinebackup.html>
- WAL-G composing the chain during a fetch: <https://github.com/wal-g/wal-g/blob/master/docs/PostgreSQL.md>
- Elasticsearch snapshot and restore API: <https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-snapshot>
- Litestream's read-only VFS over object storage: <https://litestream.io/reference/vfs/>
- Industry evidence on restore mechanics and disk floors: `docs/research/backup-shapes-and-restore.md`
