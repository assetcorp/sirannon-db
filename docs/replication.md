# Distributed replication

<p align="center">
  <img src="assets/replication-topology.svg" alt="Diagram of Sirannon's coordinator-backed replication. Clients write to the current primary and read from eligible nodes. The primary replicates to the replicas over gRPC with mutual TLS. A Sirannon controller performs failover through leases and atomic term updates in etcd." width="820">
</p>

One primary accepts writes and pushes its changes to read replicas, which serve reads and forward writes to the primary when `writeForwarding` is on. Each node opens its own SQLite file, so Sirannon moves checksummed batches of changes between the nodes over a replication transport.

You'll build two files in this guide, `primary.ts` and `replica.ts`, plus a third file, `node-a.ts`, for coordinator mode. Each file is a whole program.

## Install the transport

Install the core package, a driver, and the three packages that the gRPC transport imports:

```bash
pnpm add -E @delali/sirannon-db better-sqlite3 @grpc/grpc-js @bufbuild/protobuf grpc-health-check
```

When the process cannot load one of those three packages, `engine.start()` fails with code `TRANSPORT_DEPENDENCY_MISSING` and a message that names the package.

## Create the certificates

The nodes authenticate each other over mutual TLS, so each node needs a key, a certificate, and the certificate of the authority that signed every node certificate. Set each certificate's common name to the node's `nodeId`, because a node closes the stream from a peer whose certificate common name differs from the `nodeId` in that peer's handshake. The subject alternative name must match the host name that the other node dials.

```bash
mkdir -p certs data

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -subj "/CN=Orders replication CA" \
  -keyout certs/ca.key -out certs/ca.crt

for entry in primary:primary-us-east-1:primary.example.com replica:replica-eu-west-1:replica.example.com; do
  IFS=: read -r file node host <<< "$entry"
  printf 'basicConstraints = CA:FALSE\nkeyUsage = digitalSignature, keyEncipherment\nextendedKeyUsage = serverAuth, clientAuth\nsubjectAltName = DNS:%s\n' "$host" > "certs/$file.ext"
  openssl req -newkey rsa:2048 -nodes -subj "/CN=$node" -keyout "certs/$file.key" -out "certs/$file.csr"
  openssl x509 -req -in "certs/$file.csr" -days 825 -CA certs/ca.crt -CAkey certs/ca.key -CAcreateserial \
    -extfile "certs/$file.ext" -out "certs/$file.crt"
done
```

Replace both host names with the ones that your nodes use.

## Start the primary

Open a writer connection, create the tables that you replicate, and watch each of them with a `ChangeTracker`, because the tracker records changes only on a table that you watch. Then open the same file through a `Sirannon` registry, and start a `ReplicationEngine` in the primary role:

```ts
import { ChangeTracker, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const dbPath = './data/orders.db'
const driver = betterSqlite3()

const writerConn = await driver.open(dbPath)
await writerConn.exec('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')

const tracker = new ChangeTracker()
await tracker.watch(writerConn, 'orders')

const sirannon = new Sirannon({ driver })
const db = await sirannon.open('orders', dbPath)

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

The engine records and applies changes through the writer connection. `snapshotConnectionFactory` opens a read-only connection, so a joining node can copy a consistent snapshot while the primary keeps accepting writes.

## Point a replica at the primary

The replica opens its own file and dials the primary at the address in `transportConfig.endpoints`. In static mode, a replica opens no listening port, so its transport takes only the certificate paths. During first sync, the replica watches each table that it copies, so you leave the `watch` call out of `replica.ts`:

```ts
import { ChangeTracker, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const dbPath = './data/orders-replica.db'
const driver = betterSqlite3()

const writerConn = await driver.open(dbPath)
const tracker = new ChangeTracker()

const sirannon = new Sirannon({ driver })
const db = await sirannon.open('orders', dbPath)

const transport = new GrpcReplicationTransport({
  tlsCert: './certs/replica.crt',
  tlsKey: './certs/replica.key',
  tlsCaCert: './certs/ca.crt',
})

const engine = new ReplicationEngine(db, writerConn, {
  nodeId: 'replica-eu-west-1',
  topology: new PrimaryReplicaTopology('replica'),
  transport,
  transportConfig: { endpoints: ['primary.example.com:4200'] },
  writeForwarding: true,
  changeTracker: tracker,
})

await engine.start()
```

With `writeForwarding: true`, the replica forwards a write that you send to its `engine.execute` to the primary, which then replicates it back.

With `initialSync` on, which is the default, a new node copies the whole database before it serves reads. The source streams the schema and the table data in checksummed batches, and then it sends a manifest. The joining node moves through the phases `pending`, `syncing`, `catching-up`, and `ready`, whose current value you can read from `engine.status().syncState.phase`. For a database too large to copy over the network, copy the file yourself, and then start the replica with `initialSync: false` and with `resumeFromSeq` set to the sequence that your copy reached.

## Write concerns

Pass a write concern to `engine.execute` in `primary.ts` to set how many replicas must acknowledge the write. A majority write on a primary with no connected replica fails with `WRITE_CONCERN_ERROR` once `timeoutMs` passes, so wait until a replica connects before you send one:

```ts
while (!engine.status().peers.some(peer => peer.connected)) {
  await new Promise(resolve => setTimeout(resolve, 500))
}

await engine.execute('INSERT INTO orders (id, total) VALUES (?, ?)', [1, 4999], {
  writeConcern: { level: 'majority', timeoutMs: 5000 },
})
```

Start `primary.ts` before `replica.ts`. A replica whose primary stops, keeps a reconnection timer pending, so its process stays up while that primary is away. Its transport dials the primary again after 250 ms, doubling the wait after each failed attempt to a maximum of 5,000 ms.

In static mode, a write without `writeConcern` returns after the local commit, while in coordinator mode it waits for `'majority'`. In coordinator mode, the engine counts the configured voting nodes towards `'majority'`, including the primary's own durable commit, so a majority write is still present after an automatic failover that loses only the primary.

## Read concern

Use a read concern to say how current a read must be. A node in coordinator mode enforces the read concern, while a node in static mode ignores it.

| Level | What the node must prove |
| --- | --- |
| `local` | The node proves nothing, and the read returns local state that a later failover may quarantine. |
| `majority` | The node is in the in-sync set and is neither draining nor repairing. |
| `linearizable` | The current primary answers the read after it proves live authority for its term. |

When a node can't meet the read concern, the read fails with an error code for the reason, such as `NODE_NOT_IN_SYNC`, `STALE_PRIMARY`, or `READ_CONCERN_ERROR`, and the node returns no weaker result.

```ts
const rows = await engine.query('SELECT id, total FROM orders WHERE id = ?', [1], {
  readConcern: { level: 'linearizable' },
})
```

The [topology-aware client](client.md#topology-aware-routing) reads `GET /db/{id}/cluster` to learn the levels that each node serves at that moment, and it picks an endpoint for each read from that answer.

## Coordinator-backed failover

In coordinator mode, a `ClusterCoordinator` stores the primary's authority, the node sessions, the group state, and the in-sync set. The etcd adapter in the package imports `etcd3`, so install that package:

```bash
pnpm add -E etcd3
```

When the process cannot load `etcd3`, the first call that the engine makes to the coordinator fails with code `COORDINATOR_DEPENDENCY_MISSING`.

Build each coordinator-mode node in the shape of `primary.ts`, and add a coordinator to its engine configuration. This `node-a.ts` is the first of three voting nodes, and its certificate has the common name `orders-node-a`:

```ts
import { readFileSync } from 'node:fs'
import { ChangeTracker, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { PrimaryReplicaTopology, ReplicationEngine } from '@delali/sirannon-db/replication'
import { createEtcdCoordinator } from '@delali/sirannon-db/replication/coordinator/etcd'
import { GrpcReplicationTransport } from '@delali/sirannon-db/transport/grpc'

const dbPath = './data/orders.db'
const driver = betterSqlite3()

const writerConn = await driver.open(dbPath)
await writerConn.exec('CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY, total INTEGER NOT NULL)')

const tracker = new ChangeTracker()
await tracker.watch(writerConn, 'orders')

const sirannon = new Sirannon({ driver })
const db = await sirannon.open('orders', dbPath)

const transport = new GrpcReplicationTransport({
  host: '0.0.0.0',
  port: 4200,
  tlsCert: './certs/orders-node-a.crt',
  tlsKey: './certs/orders-node-a.key',
  tlsCaCert: './certs/ca.crt',
})

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

await engine.start()
```

Your etcd cluster has its own certificate authority, so ask whoever operates that cluster for the three files under `credentials`.

Give every coordinator-mode node a stable `nodeId` that stays the same across restarts. Put at least three voting data-bearing nodes in the group for automatic write failover, because the one survivor of a two-node group can't prove majority authority. In production, connect to etcd over HTTPS with an authenticated identity, and keep the in-memory coordinator and `allowInsecure: true` for tests.

Read [Backups in a replication group](backups.md#backups-in-a-replication-group) for how the same coordinator picks the node that takes the group's backups.

## Conflict resolution

When a receiving node finds that the target row already exists, it passes the local and the incoming versions to the resolver that you configured.

| Strategy | Class | Behaviour |
| --- | --- | --- |
| Last-Writer-Wins | `LWWResolver` | It accepts a remote delete whatever the timestamps say, so it applies a delete over a concurrent update. For any other change, it takes the version with the higher HLC timestamp and breaks a tie by node ID. |
| Field-Level Merge | `FieldMergeResolver` | It merges the columns that only one side changed and uses per-column HLC metadata for a column that both sides changed. Without column metadata, it resolves the whole row by last-writer-wins. |
| Primary Wins | `PrimaryWinsResolver` | It takes the version that the configured primary node wrote, and it resolves any other conflict by last-writer-wins. |

Write a custom resolver as a class with a `resolve(ctx: ConflictContext): ConflictResolution` method.

## Transports

| Transport | Import | Use case |
| --- | --- | --- |
| gRPC | `@delali/sirannon-db/transport/grpc` | Production multi-node replication over the network with TLS |
| In-Memory | `@delali/sirannon-db/transport/memory` | Tests and single-process multi-node setups |
| Custom | Your own module | Any class that implements the `ReplicationTransport` interface |

A `ReplicationTransport` moves change batches, acknowledgements, forwarded writes, and first-sync data between nodes. The client's `Transport` interface is a separate contract, which you'll find in the [client guide](client.md).

## Common questions

### Is this SQLite over a shared network file system?

No, each node opens its own local SQLite file. Sirannon moves changes between the nodes through a replication transport, while applications reach the data through the HTTP and WebSocket server.

### What kind of replication is it?

Sirannon uses change-log replication. It captures each local write and stamps it with a Hybrid Logical Clock. It then groups those writes into checksummed `ReplicationBatch` messages, which each replica applies by primary key.

A replicated change holds the table, the operation, the primary key, the old row, the new row, the transaction identifier, the node identifier, and the HLC. A replica applies those row values directly, replaying SQL only for the schema changes on the allowlist further down this page. A single primary accepts every ordinary write, and Sirannon uses no CRDT.

The WebSocket connection is for applications, which send queries, writes, and CDC subscriptions over it. In production, nodes replicate to each other over `GrpcReplicationTransport`.

### What conflict model does it use?

One primary per replication group serialises the ordinary writes before replication. When a receiving node applies a batch and finds that a target row already exists, it calls the resolver that you configured. The built-in choices are last-writer-wins by HLC, `PrimaryWins`, and `FieldMerge` with per-column HLCs.

The package offers no command to merge a divergent former primary back into the group. In coordinator mode, Sirannon quarantines a former primary that holds local-only writes and takes it out of service. An operator then rebuilds or restores that node before it rejoins.

### What happens under a network partition?

Static primary-replica mode has no failover of its own, so nobody can write until an operator or an external system promotes another node and reroutes the clients. In coordinator mode, the controller works from the cluster coordinator's primary terms, node leases, and in-sync sets, and it promotes only a replica that is provably in sync. When Sirannon can't prove that a primary is safe, every write fails with an error code.

### What does majority write concern mean?

In coordinator mode, Sirannon counts the configured voting data-bearing nodes in the replication group towards `majority`, including the primary's own durable commit. A majority write is still present after an automatic failover that loses only the primary, as long as an eligible in-sync replica remains.

### Does it replicate schema changes?

Yes, Sirannon replicates the schema changes on its safety allowlist, which holds `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX`. It refuses a DDL statement that contains several statements, `AS SELECT`, `ATTACH`, extension loading, or another unsafe pattern.

### What happens with foreign keys and unique constraints?

SQLite enforces the constraints on each node, so every replicated change must satisfy them there. Because one primary serialises the writes, two concurrent writes can't conflict on a unique key in normal operation. During first sync, the source sends the tables in foreign-key order. A joining node turns foreign keys off while it wipes its tables and copies the new data, and it turns them back on once the copy completes.

### Is Sirannon local-first or multi-writer today?

The production path is primary-replica. You pick a conflict resolver for how a receiving node applies a change to an existing row, and the replication engine stays single-writer whichever one you pick. For offline-first end-user devices, read the [device sync guide](device-sync.md).

The `ReplicationOptions`, `CoordinatorModeConfig`, and `TransportConfig` tables are in the [configuration reference](configuration.md).
