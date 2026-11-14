# A former primary rolls back the writes that only it held, and keeps a copy of each one

In coordinator mode, a former primary that returned with writes that no other voter held used to mark itself faulted, and it stayed out of service until an operator rebuilt it. Under sustained `majority` load, almost every primary crash leaves a few such writes in flight, so in a stress run every failover quarantined the former primary and the group ran one copy short until an operator stepped in. MongoDB and Kafka both rejoin such a node without an operator, so Sirannon now does the same, and `packages/spec/09-failover.md` carries the rules.

## Why this over the alternatives

- **Keeping the quarantine.** It keeps every undone write in place, but it leaves the group without that copy after each failover, and the copy returns only when an operator acts.
- **Truncating without a copy, as Kafka does.** Kafka's KIP-101 truncates a returning replica's log to the leader's epoch and keeps nothing, which suits a log of events. A Sirannon row can hold a write that a person needs to see again, so Sirannon keeps a copy of each undone write, as MongoDB writes its rollback files.

## Decision

- Promotion removes the former primary from the in-sync set and leaves it out of the repairing set.
- A former primary reconciles its history with the new primary's as any replica does: it undoes, from the row images in its change log, every history change after the last transaction that both nodes hold.
- In the same transaction it copies each undone change into `_sirannon_rolled_back_changes`. A copy in a system table commits with the undo and stays encrypted with the rest of the file, whereas a separate file next to the database would do neither.
- A full copy keeps every local history change that is newer than the copy, because the copy replaced the rows that those changes wrote. A reconciliation that falls back to a full copy because of a schema change keeps the changes after the shared transaction first.
- `rolledBackChanges(engine)` returns the kept changes and `clearRolledBackChanges(engine)` deletes them, and both are named exports of `@delali/sirannon-db/replication`, so that a bundler drops them when the caller imports neither.
- An operator can still mark a node faulted, which keeps the quarantine rules for that node.

## Cost

An application that reads with `local` concern from a primary can see a write that a later failover undoes, which matches what MongoDB documents for its rollback. The rows in `_sirannon_rolled_back_changes` stay until an operator deletes them, so a group that fails over often keeps growing that table. A full copy keeps only the local changes that are newer than the copy, so a node that a restart proof sends to a full copy can lose a write that it never acknowledged and that is older than the copy.

## References

- MongoDB rollbacks during replica set failover: <https://www.mongodb.com/docs/manual/core/replica-set-rollbacks/>
- Kafka KIP-101, truncation by leader epoch: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation>
