# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, migrations, backups, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, while Sirannon nodes replicate primary-owned changes over gRPC. End-user devices sync a whole local database offline-first over the same server. Coordinator mode adds etcd-backed authority and automatic failover.

Read the full documentation at [sirannon.sondelali.com/docs](https://sirannon.sondelali.com/docs).

The benchmarks compare Sirannon against Postgres 17 on the same OLTP workloads, driving each engine through the client it provides and matching durability on both sides. Every published figure is generated from a recorded run on a disclosed machine, and the page shows where each engine wins. See the full [methodology and results](BENCHMARKS.md).

See a three-node cluster keep serving through a primary failure in the [distributed entitlements example](packages/ts/examples/distributed-entitlements/), which runs the etcd coordinator, gRPC replication with mutual TLS, and fault injection on your machine.

> *sirannon* means 'gate-stream' in Sindarin.

## Project status

Sirannon has two levels of maturity. The core data layer, the server, the client, and primary-replica replication are stable. Coordinator-backed automatic failover is the newest part and needs more production use before it is stable.

| Part | Status | Details |
| --- | --- | --- |
| Core engine ([`@delali/sirannon-db`](packages/ts/)) | Stable | Queries, transactions, connection pooling, change data capture, migrations, backups, hooks, metrics, and multi-tenant lifecycle, covered by more than 130 test files with continuous integration on Node 22 and 24. |
| Server and client (`@delali/sirannon-db/server`, `@delali/sirannon-db/client`) | Stable | HTTP and WebSocket access with reconnection and subscription restore. The server runs client SQL by design, so read the [security section](packages/ts/README.md#security) before you expose it. |
| Device sync (`@delali/sirannon-db/client`) | Experimental | Offline-first, bidirectional sync between an end-user device's local database and a server, with push, live pull, snapshot resync, a migration handshake, and capability negotiation. It is new and not yet proven in production. |
| Primary-replica replication (`@delali/sirannon-db/replication`) | Stable | Hybrid Logical Clock stamping, conflict resolvers, first sync, write concerns, and a gRPC transport with mutual TLS. |
| Coordinator-backed automatic failover (`@delali/sirannon-db/replication/coordinator/etcd`) | Experimental | etcd authority, primary terms, and in-sync sets, verified by a Docker conformance run under fault injection. It is new and not yet proven in production. |
| Drivers | Stable: better-sqlite3, Node, wa-sqlite. Experimental: Bun, Expo | The Bun and Expo drivers run today but have no TypeScript declarations yet. |

Durability follows SQLite's WAL mode with `synchronous=NORMAL` by default, and you can raise it. The [roadmap](ROADMAP.md) sets out what is next, including a second-language implementation and scaling beyond a single node's disk.

## Install

```bash
pnpm add -E @delali/sirannon-db
```

Then add the [driver](#pluggable-drivers) for your runtime. For example, Node.js users will typically add `better-sqlite3`:

```bash
pnpm add -E better-sqlite3
```

## Quick start (Node.js)

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const driver = betterSqlite3()
const sirannon = new Sirannon({ driver })
const db = await sirannon.open('app', './data/app.db')

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

## Pluggable drivers

Sirannon-db separates the database engine from the library. Pick the driver that fits your runtime:

| Driver | Import | Runtime |
| --- | --- | --- |
| better-sqlite3 | `@delali/sirannon-db/driver/better-sqlite3` | Node.js |
| Node built-in | `@delali/sirannon-db/driver/node` | Node.js >= 22 (`--experimental-sqlite`) |
| wa-sqlite | `@delali/sirannon-db/driver/wa-sqlite` | Browser (IndexedDB persistence) |
| Bun | `@delali/sirannon-db/driver/bun` | Bun |
| Expo | `@delali/sirannon-db/driver/expo` | React Native |

## Package exports

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters |
| `@delali/sirannon-db/file-migrations` | Load `.up.sql` / `.down.sql` files from a directory |
| `@delali/sirannon-db/backup-scheduler` | Cron-scheduled backup runner with file rotation, also re-exported from the core entry |
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser/Node.js client SDK with auto-reconnect, subscription restore, and the device sync controller |
| `@delali/sirannon-db/replication` | Replication engine, primary-replica topology, HLC, write concerns, and conflict resolvers |
| `@delali/sirannon-db/replication/coordinator/etcd` | etcd-backed cluster coordinator for primary authority and automatic failover |
| `@delali/sirannon-db/transport/grpc` | gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | In-memory replication transport for tests and single-process clusters |

## Features

- **Queries and transactions** - Execute reads, writes, and batch operations. Transactions provide full ACID guarantees.
- **Bulk load** - Load a large dataset in one transaction under relaxed durability, then restore the configured level, so a big import pays one durability barrier instead of one per row.
- **Connection pooling** - 1 dedicated write connection + N read connections (default 4). WAL mode enabled by default for concurrent reads during writes.
- **Change data capture (CDC)** - Watch tables for INSERT, UPDATE, and DELETE events in real time through SQLite triggers and configurable polling.
- **Migrations** - File-based (numbered `.up.sql` / `.down.sql`) or programmatic migrations are tracked with content checksums in a `_sirannon_migrations` table and mirrored to `PRAGMA user_version`. You can roll back to any version, squash a long history into one baseline migration, and let two processes migrate the same database at once: one wins, the other retries and skips what already ran. A migration set declared on the registry applies to every database it opens, including lazily resolved tenants. The [TypeScript README](packages/ts/README.md#migrations) holds the full guide.
- **Backups** - One-shot snapshots via `VACUUM INTO` and scheduled backups on a cron expression with automatic file rotation.
- **Hooks** - Before/after hooks for queries, connections, and subscriptions. Throwing from a before-hook denies the operation.
- **Metrics** - Plug in callbacks to collect query timing, connection events, and CDC activity.
- **Lifecycle management** - Auto-open databases on first access with idle timeouts and LRU eviction for multi-tenant setups.
- **Server** - Expose any `Sirannon` instance over HTTP and WebSocket with a single function call. Query, execute, transaction, batch, and bulk-load routes on both transports, plus health endpoints, CORS configuration, a configurable body-size cap, and an `onRequest` hook for authentication.
- **Client SDK** - Async API mirroring the core `Database` interface. Supports HTTP and WebSocket transports with automatic reconnection and subscription restoration.
- **Device sync** - Keep an end-user device's local database in step with a server, offline-first and bidirectional. A device pushes its own writes and pulls other writes live over WebSocket, with snapshot resync, a migration handshake, and capability negotiation. See the [device sync guide](packages/ts/README.md#device-sync).
- **Distributed replication** - Replicate HLC-stamped change batches from a primary node to read replicas. The production network transport is gRPC with TLS support.
- **Coordinator-backed failover** - Use etcd-backed authority, primary terms, in-sync sets, and write concerns. Minority partitions fail closed for writes.
- **Deterministic batch application** - Choose LWW, PrimaryWins, FieldMerge, or a custom resolver when an incoming replicated change targets an existing row.

## Examples

Self-contained example projects in [`packages/ts/examples/`](packages/ts/examples/) cover the current Node.js, browser, client-server, and distributed paths:

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](packages/ts/examples/node/) | Node.js >= 22 | All core features with either `better-sqlite3` or Node's built-in SQLite driver |
| [`web-wa-sqlite`](packages/ts/examples/web-wa-sqlite/) | Browser (Vite) | CRUD, transactions, and CDC in the browser |
| [`web-client`](packages/ts/examples/web-client/) | Browser + Node.js | Client SDK connecting to a Sirannon server over HTTP and WebSocket |
| [`distributed-entitlements`](packages/ts/examples/distributed-entitlements/) | Node.js + browser | Three-node coordinator-backed replication with etcd, gRPC, local mTLS certificates, and Toxiproxy failure controls |

```bash
pnpm install && pnpm --filter @delali/sirannon-db build

# then pick one:
cd packages/ts/examples/node && pnpm start
cd packages/ts/examples/node && pnpm run start:node-native
cd packages/ts/examples/web-wa-sqlite && pnpm run dev
cd packages/ts/examples/web-client && pnpm run dev
cd packages/ts/examples/distributed-entitlements && pnpm run dev
```

## Architecture

Application clients reach the primary and read replicas over HTTP and WebSocket. The primary accepts every write, assigns each change a Hybrid Logical Clock timestamp, and sends checksummed batches to the replicas over gRPC with mutual TLS. An etcd coordinator tracks primary authority, node leases, and the in-sync set, and promotes an in-sync replica when the primary fails.

<p align="center">
  <img src="docs/assets/replication-topology.svg" alt="Sirannon replication topology: application clients reach the primary and read replicas, the primary replicates to replicas over gRPC with mutual TLS, and an etcd coordinator tracks authority, leases, and the in-sync set." width="820">
</p>

## Device sync

Device sync keeps an end-user device's local database in step with a server database, offline-first and bidirectional. A device pushes its own writes and pulls other writes live over the WebSocket, applying both sides through the same conflict resolvers replication uses. Each user gets their own database file, and a device syncs the whole database. A fresh device, or one too far behind to resume, resyncs from a server snapshot; a device applies schema changes through a migration handshake, and a client refuses to sync against a server that does not advertise device sync. Device sync is distinct from server-to-server replication: a device is not a replication peer and holds no primary authority. The [device sync guide](packages/ts/README.md#device-sync) covers setup, and the [specification](packages/spec/08-device-sync.md) defines the wire protocol.

## Distributed replication FAQ

### Is Sirannon SQLite over a shared network file system?

No. Each node has its own local SQLite database file. Sirannon moves changes between nodes through a replication transport and exposes database operations to applications through the HTTP/WebSocket server and client SDK. Each process opens its own file instead of sharing one SQLite file over NFS or another network file system.

### What kind of replication does Sirannon use?

Sirannon uses change-log replication. Local writes are captured, stamped with a Hybrid Logical Clock (HLC), grouped into checksummed `ReplicationBatch` messages, and applied on replicas by primary key. The transport can be in-memory for tests or gRPC with TLS for Node.js clusters.

WebSocket serves a different purpose. It is a client transport for application queries, writes, and CDC subscriptions. It does not move `ReplicationBatch` messages between Sirannon nodes. Production node-to-node replication uses `GrpcReplicationTransport`.

### Is it row-based, statement-based, operation-log based, or CRDT-like?

It is operation-log based at the Sirannon layer. A replicated change includes the table, operation, primary key, old data, new data, transaction ID, node ID, and HLC. It is not raw SQL statement replay, and the current production write path is not a CRDT protocol.

### What conflict model does it use?

Normal writes use a single primary per replication group, so writes are serialised before replication. During batch application, the receiver invokes a conflict resolver whenever the target row already exists. The built-in choices are Last-Writer-Wins by HLC, PrimaryWins, and FieldMerge with per-column HLCs.

The package does not expose a high-level command that merges a divergent former primary back into the group. Coordinator mode quarantines a former primary with local-only writes and removes it from safe service. Recovery then requires an operator to rebuild, restore, or otherwise remediate that node before it can rejoin.

### What happens under network partitions?

Static primary-replica mode has no Sirannon-owned failover; writes stay unavailable until an operator or external system promotes another node and reroutes clients. Coordinator mode uses a cluster coordinator, primary terms, node leases, in-sync sets, and fail-closed write behaviour. Only a proven in-sync replica can become primary. If Sirannon cannot prove a safe primary, writes fail with a clear error.

### What does majority write concern mean?

In coordinator mode, `majority` is calculated from configured voting data-bearing nodes in the replication group, including the primary's local durable commit. A successful `majority` write survives automatic primary failover when only the failed primary is lost and an eligible in-sync replica remains.

Coordinator mode uses `majority` when a write does not specify a concern. Static mode returns after the local commit unless the write requests a stronger concern.

### Does Sirannon replicate schema changes?

Yes, within a safety allowlist. Replicated DDL supports `CREATE TABLE`, `ALTER TABLE ... ADD COLUMN`, `DROP TABLE`, `CREATE INDEX`, and `DROP INDEX`. DDL with multiple statements, `AS SELECT`, `ATTACH`, extension loading, and other dangerous patterns is rejected.

### What happens with foreign keys and unique constraints?

SQLite enforces constraints on each node. The single-primary write path prevents normal concurrent unique-key conflicts. First sync orders tables by foreign-key dependency, and resync disables foreign keys only during the controlled table-wipe phase. Incoming replicated data still has to satisfy the receiving database's constraints.

### Is Sirannon local-first or multi-writer today?

The current production path is primary-replica. Conflict resolvers determine how a receiving node applies a change to an existing row; they do not turn the replication engine into a multi-writer or CRDT system.

## Security

Sirannon-db is designed to be secure by default in its core operations:

- **Parameterised queries** - All SQL execution uses parameter binding through the driver layer, preventing SQL injection.
- **Identifier validation** - CDC table and column names are validated against a strict allowlist regex (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`).
- **Path traversal prevention** - Migration and backup paths reject null bytes, `..` segments, and control characters.
- **Request size limits** - HTTP bodies and WebSocket payloads are capped at 1 MB.

> **Warning:** The server accepts arbitrary SQL from clients. When you expose it beyond localhost, always use the `onRequest` hook to authenticate and authorise requests. See the [TypeScript package docs](packages/ts/README.md#security) for examples.
>
> The built-in server binds plain HTTP and WebSocket without TLS. When you serve traffic outside a trusted network, terminate TLS upstream with a reverse proxy (nginx, Caddy, a cloud load balancer) or your clients' bearer tokens and query payloads will be sent in cleartext.

## Documentation

Full API reference, code examples, and configuration tables are in the [TypeScript package README](packages/ts/README.md).

## Benchmarks

The benchmark suite compares Sirannon against Postgres 17 on the same OLTP workloads: point-select, bulk-insert, batch-update, YCSB A/B/C/F, and a TPC-C-shaped mix. It drives Sirannon over HTTP into its real server and Postgres over its socket, both as native processes on pinned cores under a hard memory ceiling at matched durability, under an open-loop load generator that corrects for coordinated omission. It also records Sirannon-only characterizations: change-feed latency, cold start, and connection scaling. The harness is a Python project under [`benchmarks/server`](benchmarks/server); see [`BENCHMARKS.md`](BENCHMARKS.md) for the methodology and the latest results.

## Development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

## License

Apache-2.0
