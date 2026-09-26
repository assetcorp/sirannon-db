# Replication across nodes

A primary stamps each change with a hybrid logical clock and sends checksummed batches of changes to its read replicas over gRPC with mutual TLS. Primary-replica replication is stable. Automatic failover through etcd is experimental, so tell the user before you build on it. Replication spreads reads across nodes, while every write still goes through the one primary.

## Install the peer packages

| Import | What it gives you | Install |
| --- | --- | --- |
| `@delali/sirannon-db/replication` | `ReplicationEngine`, `PrimaryReplicaTopology`, and the conflict resolvers | Nothing |
| `@delali/sirannon-db/transport/grpc` | `GrpcReplicationTransport`, with TLS | `pnpm add -E @grpc/grpc-js @bufbuild/protobuf grpc-health-check` |
| `@delali/sirannon-db/transport/memory` | `InMemoryTransport` and `MemoryBus`, for tests and one-process clusters | Nothing |
| `@delali/sirannon-db/replication/coordinator/etcd` | `createEtcdCoordinator`, for primary authority and automatic failover | `pnpm add -E etcd3` |
| `@delali/sirannon-db/client/topology` | `TopologyAwareClient`, which sends writes to the primary and reads to eligible replicas | Nothing |

When a package is missing, the gRPC transport throws `TRANSPORT_DEPENDENCY_MISSING` as it connects and names the missing package, and the etcd coordinator throws `COORDINATOR_DEPENDENCY_MISSING` on its first call. Version 0.3.3 and older fail at the import instead.

## Build it from the versioned documentation

A replication group needs a node identity, certificates for mutual TLS, a topology, and a transport on every node, and the setup differs between a fixed primary and a coordinator that elects one. Read `distributed-replication.md` and `topology-routing.md` in the versioned documentation, and take every constructor option from the installed `.d.ts` for `@delali/sirannon-db/replication` and the transport that you chose.

- Test a group first on `InMemoryTransport`, which needs no certificates, and switch to `GrpcReplicationTransport` once the behaviour is right.
- A write concern of `'majority'` or `'all'` waits for replicas to confirm a write, and `readConcern` on a query selects how current a read must be. Both are options of `db.execute` and `db.query`.
- Give every node the same `backups` option, with `replicationGroup` and `preferredNode` set, so that one node takes the backups for the group. `backups-in-a-cluster.md` in the versioned documentation shows the settings.
