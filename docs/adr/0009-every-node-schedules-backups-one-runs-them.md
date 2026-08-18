# Every node schedules backups, and the coordinator picks the one that runs

In a replicated deployment, the backup schedule must exist on every node, and each scheduled run must ask the coordinator one question before proceeding: is this node the preferred backup node for this group right now? One node answers yes, runs the backup, and the rest skip. A failover changes which node answers yes and changes nothing about any schedule.

SQL Server is the only established system that publishes a designed answer to backup scheduling across a failover, and this is its design: identical jobs on every replica, each guarded by a shared preference function, so that after a failover none of the jobs needs to be modified.

## Why this over the alternatives

- **Scheduling on the primary only** loads the node that serves every write with hours of copy I/O, and every vendor in the industry evidence points the other way, taking backups on a replica where the deployment allows it.
- **Scheduling on one fixed replica** breaks the moment that replica fails or is promoted, and someone has to notice and move the schedule by hand.
- **A coordinator-owned scheduler** would make backups depend on coordinator features Sirannon does not have, where the guard question needs only what the coordinator already knows: group membership and roles.

## Decision

- The preferred backup node is replica-preferred by default, and the operator can pin it to a named node or to the primary. This follows the unanimous industry position that replicas take the backups and the primary keeps serving writes.
- A backup taken on a replica is a valid database of the same logical content as the primary, and it is bound to that replica's chain. The per-node chain rule from the chain decision applies: after a failover, the new preferred node starts a fresh chain with a full copy.
- The server exposes the backup surface over its existing authenticated routes: trigger a backup, read progress, list what exists, verify a piece, and ask what is safe to delete. Elasticsearch's snapshot API is the model, and the one route it offers that Sirannon gates separately is restore, which the restore decision puts behind an explicit opt-in flag.
- Single-node deployments answer the guard question trivially, so the same schedule and the same code run everywhere, and adding a coordinator later changes which node proceeds rather than how backups work.

## Cost

The guard question adds a coordinator read to every scheduled tick in coordinator mode, and a partitioned node that cannot reach the coordinator skips its run and reports the skip. A deployment whose replicas all lag badly could take its backup from a node that is behind the primary, and the chain position in the completion report is what makes that visible.

## References

- SQL Server backup preference and the per-replica guard function: <https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/configure-backup-on-availability-replicas-sql-server>
- Elasticsearch snapshot API surface: <https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-snapshot>
- Industry evidence on backup placement in replicated deployments: `docs/research/backup-shapes-and-restore.md`
