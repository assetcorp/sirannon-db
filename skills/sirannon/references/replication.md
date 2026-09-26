# Replication across nodes

Replication adds read replicas behind one primary, and every write still goes to the primary. Primary-replica replication is stable, while automatic failover through etcd is experimental, so confirm with the user before you build failover.

1. Install the transport's peers with `pnpm add -E @grpc/grpc-js @bufbuild/protobuf grpc-health-check`, and `pnpm add -E etcd3` for failover.
2. Read `distributed-replication.md` in the versioned documentation, and take every option from the installed types for `@delali/sirannon-db/replication`, `/transport/grpc`, and `/replication/coordinator/etcd`.
3. Build the group on `InMemoryTransport` from `@delali/sirannon-db/transport/memory` first, since it needs no certificates. Switch to `GrpcReplicationTransport` with mutual TLS once the group behaves.
4. Route clients with `TopologyAwareClient` from `@delali/sirannon-db/client/topology`, which sends writes to the primary and reads to eligible replicas.

A missing peer makes the gRPC transport throw `TRANSPORT_DEPENDENCY_MISSING` when it connects, and makes the etcd coordinator throw `COORDINATOR_DEPENDENCY_MISSING` on its first call. On 0.3.3, the import itself fails.
