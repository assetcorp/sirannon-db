# A majority write waits for the in-sync set, and the primary alone changes that set

In coordinator mode, the primary used to record every `majority` write in etcd before it acknowledged the write. Each record replaced the in-sync set with the voters that had acknowledged that one write, so under concurrent load a voter that was a few milliseconds behind left the set and rejoined it on the next write. In a three-voter drill, between 27 and 43 of 5,400 writes that a majority already held came back failed with `COORDINATOR_UNAVAILABLE`, and a write sent on its own cost two etcd transactions. This decision moves Sirannon to the model that Kafka uses for its in-sync replicas, and `packages/spec/09-failover.md` carries the rules.

## Why this over the alternatives

- **Batching the per-write record.** Batching lowers the number of etcd transactions, but every batch still rewrites the in-sync set from one sample of acknowledgements, so the set keeps flapping under load.
- **Counting any majority of voters, as Raft does.** Raft can count any majority because its election picks the copy with the newest log. Sirannon's controller promotes only from the in-sync set, so a write that a majority holds outside that set can be lost when the controller promotes a set member that lacks it.

## Decision

- A `majority` write waits until every copy in the in-sync set holds it, and the primary acknowledges it only while that set holds most of the voters. The primary writes nothing to etcd for the write.
- Only the primary admits a copy to the set, once the copy holds both the durability point and the highest write that the primary has acknowledged. From the moment that the primary asks, every write waits for that copy too, but only the voters that etcd records in the set count towards the majority, as Kafka counts only its committed in-sync set against `min.insync.replicas`. A replica outside the set reports its position every second, so that the primary can admit it when no writes arrive.
- The primary removes a copy that leaves a change unacknowledged for more than `coordinator.maxReplicaLagMs`, which defaults to 1,000 milliseconds. It removes only as many voters as leave a majority in the set, most behind first, and etcd refuses any removal below that floor.
- The group records the last term in which the in-sync set held a majority, so that a restarted primary knows whether that term can hold an acknowledged write.
- A restarted primary accepts no write until it proves that it lost no acknowledged write, and it then keeps its duty in a new term. While another voter stays in the set, one report from that voter at or below the primary's newest write settles the proof. Once every other voter has left the set, for instance because each one restarted, the primary needs reports from enough of the remaining voters to include one that held every acknowledged write. This follows Kafka's KIP-966, which stops a last in-sync replica that shut down uncleanly from taking the lead back unchecked.
- A group holds at most seven voters.
- The group records a feature level, which an operator raises once every copy's session supports the new level, after the pattern of Kafka's `metadata.version` and MongoDB's feature compatibility version. A new group starts at level 2. A group at level 1 keeps the earlier rules, so that a rolling upgrade from 0.4 keeps working.

## Cost

A stalled replica delays every `majority` write by up to `coordinator.maxReplicaLagMs` before the primary removes it. When two copies of a three-voter group drain or stall, `majority` writes fail until one of them returns, which matches what Kafka does once fewer replicas than `min.insync.replicas` remain. A copy that runs 0.4 drops the feature level when it rewrites the group record, so an operator must not start 0.4 copies once the group runs at level 2. When a restarted primary that is the last copy in the set finds another voter ahead of it, the primary marks itself repairing and the group has no safe primary until an operator writes the group's state. Kafka's KIP-966 goes further at that point and elects the copy with the longest log, which Sirannon's promotion path cannot do yet.

## References

- Kafka KIP-497, the leader's requests to change the in-sync set: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-497%3A+Add+inter-broker+API+to+alter+ISR>
- Kafka KIP-966, eligible leader replicas: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas>
- Kafka replication design: <https://kafka.apache.org/documentation/#replication>
- MongoDB feature compatibility version: <https://www.mongodb.com/docs/manual/reference/command/setFeatureCompatibilityVersion/>
