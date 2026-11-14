# A former primary rolls back the writes that only it held, and keeps a copy of each one

In coordinator mode, a former primary that returned with writes that no other voter held used to mark itself faulted, and it stayed out of service until an operator rebuilt it. Under sustained `majority` load, almost every primary crash leaves a few such writes in flight, so in a stress run every failover quarantined the former primary and the group ran one copy short until an operator stepped in. MongoDB and Kafka both rejoin such a node without an operator, so Sirannon now does the same, and `packages/spec/09-failover.md` carries the rules.

## Why this over the alternatives

- **Keeping the quarantine.** It keeps every undone write in place, but it leaves the group without that copy after each failover, and the copy returns only when an operator acts.
- **Truncating without a copy, as Kafka does.** Kafka's KIP-101 truncates a returning replica's log to the leader's epoch and keeps nothing, which suits a log of events. A Sirannon row can hold a write that a person needs to see again, so Sirannon keeps a copy of each undone write, as MongoDB writes its rollback files.

## Decision

- Promotion removes the former primary from the in-sync set and leaves it out of the repairing set.
- A former primary reconciles its history with the new primary's as any replica does: it undoes, from the row images in its change log, every history change after the last transaction that both nodes hold.
- In the same transaction it copies each undone change into `_sirannon_rolled_back_changes`. A copy in a system table commits with the undo and stays encrypted with the rest of the file, whereas a separate file next to the database would do neither.
- A node keeps its rows until it has kept every change that the primary can lack, and only then starts a full copy. Before a repair copy, it asks the primary where their histories meet, as a former primary does, and keeps every change after that point. A primary records that its file holds a complete copy, so that it keeps its rows when it restarts as a replica.
- When the primary no longer keeps the history to compare against, the node keeps every change that the primary did not write and marks those copies as not compared. MongoDB discards a stale member's data when an operator resyncs it, so this keeps more than MongoDB does.
- A faulted node, and a repairing node that has not finished its copy, apply no batch, so that the changes that they keep hold only their own history.
- `rolledBackChanges(engine)` returns the kept changes and `clearRolledBackChanges(engine)` deletes them, and both are named exports of `@delali/sirannon-db/replication`, so that a bundler drops them when the caller imports neither.
- An operator can still mark a node faulted, which keeps the quarantine rules for that node.

## Cost

An application that reads with `local` concern from a primary can see a write that a later failover undoes, which matches what MongoDB documents for its rollback. The rows in `_sirannon_rolled_back_changes` stay until an operator deletes them, so a group that fails over often keeps growing that table. A node that returns after the primary has pruned the shared history keeps up to the change log's retention of changes, most of which the primary also holds, and marks them as not compared.

## References

- MongoDB rollbacks during replica set failover: <https://www.mongodb.com/docs/manual/core/replica-set-rollbacks/>
- MongoDB resync of a stale member: <https://www.mongodb.com/docs/manual/tutorial/resync-replica-set-member/>
- Kafka KIP-101, truncation by leader epoch: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation>
