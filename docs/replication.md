# Distributed replication

<p align="center">
  <img src="assets/replication-topology.svg" alt="Sirannon replication topology: application clients reach the primary and read replicas, the primary replicates to replicas over gRPC with mutual TLS, and an etcd coordinator tracks authority, leases, and the in-sync set." width="820">
</p>

One primary accepts writes and pushes changes to read replicas, which serve reads and forward writes when `writeForwarding` is on. Each node has its own SQLite file; Sirannon moves checksummed batches of changes over the replication transport and never shares one file over a network filesystem.

```ts
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const transport = new GrpcReplicationTransport({
  host: '0.0.0.0',
  port: 4200,
  tlsCert: './certs/primary.crt',
  tlsKey: './certs/primary.key',
  tlsCaCert: './certs/ca.crt',
})

const engine = new ReplicationEngine(db, writerConn, {
  nodeId: 'primary-us-east-1',
  topology: new PrimaryReplicaTopology('primary'),
  transport,
  snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
  changeTracker: tracker,
})

await engine.start()
```

A replica points at the primary's endpoints:

```ts
const replicaEngine = new ReplicationEngine(replicaDb, replicaConn, {
  nodeId: 'replica-eu-west-1',
  topology: new PrimaryReplicaTopology('replica'),
  transport: replicaTransport,
  transportConfig: { endpoints: ['primary.example.com:4200'] },
  writeForwarding: true,
  changeTracker: replicaTracker,
})

await replicaEngine.start()
```

With `initialSync` on (the default), a new node pulls a full snapshot before it serves reads: the source streams schema and table data in batches with per-batch checksums, then a manifest, and the joiner moves through `pending` -> `syncing` -> `catching-up` -> `ready`, which you read from `engine.status().syncState`. For a database too large to transfer, copy the file and start from a known sequence with `initialSync: false` and `resumeFromSeq`.

Write concerns control how many replicas must acknowledge a write:

```ts
await engine.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [1, 4999], {
  writeConcern: { level: 'majority', timeoutMs: 5000 },
})
```

Static mode returns after the local commit when you omit `writeConcern`; coordinator mode selects `'majority'`. Coordinator-mode majority counts the configured voting nodes, including the primary's own durable commit, so such a write survives automatic failover when only the failed primary is lost.

## Coordinator-backed failover

Coordinator mode stores primary authority, node sessions, group state, and the in-sync set in a `ClusterCoordinator`. The package includes an etcd adapter:

```ts
import { createEtcdCoordinator } from '@delali/sirannon-db/replication/coordinator/etcd'

const coordinator = createEtcdCoordinator({
  hosts: ['https://etcd-1.internal:2379', 'https://etcd-2.internal:2379'],
  keyPrefix: '/sirannon/orders',
  credentials: {
    rootCertificate: readFileSync('./certs/etcd-ca.crt'),
    privateKey: readFileSync('./certs/orders-node.key'),
    certChain: readFileSync('./certs/orders-node.crt'),
  },
})

const engine = new ReplicationEngine(db, writerConn, {
  nodeId: 'orders-node-a',
  topology: new PrimaryReplicaTopology('primary'),
  transport,
  transportConfig: { endpoints: ['orders-node-b.internal:4200', 'orders-node-c.internal:4200'] },
  changeTracker: tracker,
  snapshotConnectionFactory: () => driver.open(dbPath, { readonly: true }),
  writeForwarding: true,
  coordinator: {
    clusterId: 'commerce-production',
    groupId: 'orders',
    endpoint: 'https://orders-node-a.internal/db/orders',
    coordinator,
    votingDataBearingNodeIds: ['orders-node-a', 'orders-node-b', 'orders-node-c'],
    controller: true,
  },
})
```

Every coordinator-mode node needs a stable, persisted `nodeId`, and automatic write failover needs at least three voting data-bearing nodes, since fewer voters cannot prove majority authority after losing one. Production access requires HTTPS and an authenticated identity; the in-memory coordinator and `allowInsecure: true` are for tests.

## Conflict resolution

A receiver that finds the target row already present passes the local and incoming versions to the configured resolver.

| Strategy | Class | Behaviour |
| --- | --- | --- |
| Last-Writer-Wins | `LWWResolver` | Accepts a remote delete whatever the timestamps say, so a delete wins over a concurrent update. Otherwise takes the higher HLC timestamp and breaks ties by node ID. |
| Field-Level Merge | `FieldMergeResolver` | Merges non-overlapping columns and uses per-column HLC metadata for overlapping ones. Falls back to whole-row LWW without column metadata. |
| Primary Wins | `PrimaryWinsResolver` | Takes the version authored by a configured primary node ID, and falls back to LWW otherwise. |

Write a custom resolver as a class with a `resolve(ctx: ConflictContext): ConflictResolution` method.

## Transports

| Transport | Import | Use case |
| --- | --- | --- |
| gRPC | `@delali/sirannon-db/transport/grpc` | Production multi-node replication over the network with TLS |
| In-Memory | `@delali/sirannon-db/transport/memory` | Testing and single-process multi-node scenarios |
| Custom | Build your own | Anything satisfying the `ReplicationTransport` interface |

`ReplicationTransport` carries change batches, acknowledgements, write forwarding, and first sync between nodes. The client `Transport` interface is a different contract, described in the [client guide](client.md).

The `ReplicationOptions`, `CoordinatorModeConfig`, and `TransportConfig` tables are in the [configuration reference](configuration.md).
