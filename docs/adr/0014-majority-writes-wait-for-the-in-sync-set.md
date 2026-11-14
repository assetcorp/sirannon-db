# A majority write waits for the in-sync set, and the primary alone changes that set

In coordinator mode, the primary used to record every `majority` write in etcd before it acknowledged the write. Each record replaced the in-sync set with the voters that had acknowledged that one write, so under concurrent load a voter that was a few milliseconds behind left the set and rejoined it on the next write. In a three-voter drill, between 27 and 43 of 5,400 writes that a majority already held came back failed with `COORDINATOR_UNAVAILABLE`, and a write sent on its own cost two etcd transactions. This decision moves Sirannon to the model that Kafka uses for its in-sync replicas, and `packages/spec/09-failover.md` carries the rules.

## Why this over the alternatives

- **Batching the per-write record.** Batching lowers the number of etcd transactions, but every batch still rewrites the in-sync set from one sample of acknowledgements, so the set keeps flapping under load.
- **Counting any majority of voters, as Raft does.** Raft can count any majority because its election picks the copy with the newest log. Sirannon's controller promotes only from the in-sync set, so a write that a majority holds outside that set can be lost when the controller promotes a set member that lacks it.

## Decision

- A `majority` write waits until every copy in the in-sync set holds it, and the primary acknowledges it only while that set holds most of the voters. The primary writes nothing to etcd for the write.
- Only the primary admits a copy to the set, once the copy holds both the durability point and the highest write that the primary has acknowledged, and the primary counts the copy from the moment that it asks. A replica outside the set reports its position every second, so that the primary can admit it when no writes arrive.
- The primary removes a copy that leaves a change unacknowledged for more than `coordinator.maxReplicaLagMs`, which defaults to 1,000 milliseconds, and etcd refuses a removal that leaves fewer in-sync voters than a majority.
- A restarted primary accepts no write until an in-sync voter shows that the primary lost no acknowledged write, and it then keeps its duty in a new term. It gives up its duty when every in-sync voter holds more than it does, or when its session lifetime passes without that proof.
- A group holds at most seven voters.
- The group records a feature level, which an operator raises once every copy's session supports the new level, after the pattern of Kafka's `metadata.version` and MongoDB's feature compatibility version. A new group starts at level 2. A group at level 1 keeps the earlier rules, so that a rolling upgrade from 0.4 keeps working.

## Cost

A stalled replica delays every `majority` write by up to `coordinator.maxReplicaLagMs` before the primary removes it. When two copies of a three-voter group drain or stall, `majority` writes fail until one of them returns, which matches what Kafka does once fewer replicas than `min.insync.replicas` remain. A copy that runs 0.4 drops the feature level when it rewrites the group record, so an operator must not start 0.4 copies once the group runs at level 2.

## References

- Kafka KIP-497, the leader's requests to change the in-sync set: <https://cwiki.apache.org/confluence/display/KAFKA/KIP-497%3A+Add+inter-broker+API+to+alter+ISR>
- Kafka replication design: <https://kafka.apache.org/documentation/#replication>
- MongoDB feature compatibility version: <https://www.mongodb.com/docs/manual/reference/command/setFeatureCompatibilityVersion/>
