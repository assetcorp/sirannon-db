# Sirannon owns coordinator-backed automatic failover

Sirannon must own automatic primary failover when a real cluster coordinator is configured. A coordinator-backed cluster has one active controller, elected through the coordinator, and that controller manages node leases, replication-group authority, primary terms, in-sync replicas, and safe promotion.

Without a configured coordinator, Sirannon remains a static primary-replica deployment. Static deployments may use manual or external promotion, but they do not provide Sirannon-owned automatic failover.

## Why this over the alternatives

- **Manual or external failover only** leaves write availability outside Sirannon's product contract. If the primary fails, writes stay unavailable until another system promotes a node and clients are rerouted.
- **Promotion from transport disconnects** is unsafe. A broken replication stream proves connection loss, not node death. Coordinator leases provide the authority signal; transport disconnects only affect replication progress.
- **Conflict resolution as the normal failover path** permits divergent primaries. Conflict resolvers remain useful for explicit repair and disaster recovery, but normal high-availability failover must prevent two writable primaries.
- **Building a Sirannon-specific coordinator** duplicates a solved distributed-systems role. etcd provides linearizable key-value operations, leases, watch streams, leader election building blocks, TLS, and role-scoped access control. Sirannon should use etcd as the first production coordinator backend and keep other backends behind the same coordinator contract.

## Decision

Coordinator mode adds these rules to Sirannon:

- Primary authority is scoped per replication group, not per process. A process can host databases from more than one group, and each group has its own current primary and primary term.
- A node may accept writes for a group only while it can prove current coordinator authority for that group. If authority renewal or verification fails, the node fails closed for new writes and fails in-flight writes that cannot prove current authority.
- A promotion increments the group's primary term through an atomic coordinator write. Replication messages, forwarded writes, and write handling carry the current term. Nodes reject stale terms and non-current primaries.
- A replica is promotable only when its node lease is alive, it is in the group's in-sync set, it has no known storage or sync error, and the promotion can atomically advance the primary term.
- If no replica meets the safety bar, Sirannon reports no safe primary and keeps writes unavailable. Unsafe recovery requires explicit operator action with a data-loss acknowledgement.
- Production coordinator mode defaults to majority write concern over configured voting data-bearing nodes. Majority is not calculated from currently connected peers.
- Three data-bearing Sirannon nodes are the minimum production shape for automatic write failover. One node has no failover. Two nodes can replicate, but one survivor cannot prove majority authority after the other is lost.
- etcd stores only authority metadata: controller leases, node leases, replication-group configuration, current primary, primary term, in-sync set, and compact progress markers. It does not store user rows or the full replication log.
- Coordinator access in production requires TLS, authenticated Sirannon identities, a dedicated key prefix per Sirannon cluster, and least-privilege credentials for that prefix. Insecure coordinator access is limited to explicit development or test configuration.
- Clients in coordinator mode use a starter list of Sirannon endpoints, discover the current primary for a database group, route writes to that primary, and refresh routing when failover or stale-primary errors occur.

## Cost

This decision adds a production dependency on a coordinator service, more cluster state, and more failure modes to test. The gain is that Sirannon can state a clear write-availability contract: acknowledged majority writes survive automatic primary failover when a safe in-sync replica remains available.

## References

- etcd API guarantees: <https://etcd.io/docs/v3.5/learning/api_guarantees/>
- etcd API leases: <https://etcd.io/docs/v3.7/learning/api/>
- etcd transport security: <https://etcd.io/docs/v3.6/op-guide/security/>
- etcd role-based access control: <https://etcd.io/docs/v3.6/op-guide/authentication/rbac/>
- Kubernetes Leases: <https://kubernetes.io/docs/concepts/architecture/leases/>
- MongoDB replica-set elections: <https://www.mongodb.com/docs/manual/core/replica-set-elections/>
- MongoDB replica-set write concern: <https://www.mongodb.com/docs/v8.0/core/replica-set-write-concern/>
- MongoDB read concern majority: <https://www.mongodb.com/docs/manual/reference/read-concern-majority/>
- MongoDB rollbacks during failover: <https://www.mongodb.com/docs/manual/core/replica-set-rollbacks/>
- Patroni DCS failsafe model: <https://patroni.readthedocs.io/>
