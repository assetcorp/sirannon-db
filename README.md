# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, migrations, backups, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, while Sirannon nodes replicate primary-owned changes over gRPC. Coordinator mode adds etcd-backed authority and automatic failover.

> *sirannon* means 'gate-stream' in Sindarin.

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
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser/Node.js client SDK with auto-reconnect and subscription restore |
| `@delali/sirannon-db/replication` | Replication engine, primary-replica topology, HLC, write concerns, and conflict resolvers |
| `@delali/sirannon-db/transport/grpc` | gRPC replication transport with TLS support |

## Features

- **Queries and transactions** - Execute reads, writes, and batch operations. Transactions provide full ACID guarantees.
- **Connection pooling** - 1 dedicated write connection + N read connections (default 4). WAL mode enabled by default for concurrent reads during writes.
- **Change data capture (CDC)** - Watch tables for INSERT, UPDATE, and DELETE events in real time through SQLite triggers and configurable polling.
- **Migrations** - File-based (numbered `.up.sql` / `.down.sql`) or programmatic migrations, tracked in a `_sirannon_migrations` table. Supports rollback to any version.
- **Backups** - One-shot snapshots via `VACUUM INTO` and scheduled backups on a cron expression with automatic file rotation.
- **Hooks** - Before/after hooks for queries, connections, and subscriptions. Throwing from a before-hook denies the operation.
- **Metrics** - Plug in callbacks to collect query timing, connection events, and CDC activity.
- **Lifecycle management** - Auto-open databases on first access with idle timeouts and LRU eviction for multi-tenant setups.
- **Server** - Expose any `Sirannon` instance over HTTP and WebSocket with a single function call. Includes health endpoints, CORS configuration, and an `onRequest` hook for authentication.
- **Client SDK** - Async API mirroring the core `Database` interface. Supports HTTP and WebSocket transports with automatic reconnection and subscription restoration.
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

## Distributed replication FAQ

### Is Sirannon SQLite over a shared network file system?

No. Each node owns a local SQLite database file. Sirannon moves changes between nodes through a replication transport and exposes database operations to applications through the HTTP/WebSocket server and client SDK. Each process opens its own file instead of sharing one SQLite file over NFS or another network file system.

### What kind of replication does Sirannon use?

Sirannon uses change-log replication. Local writes are captured, stamped with a Hybrid Logical Clock (HLC), grouped into checksummed `ReplicationBatch` messages, and applied on replicas by primary key. The transport can be in-memory for tests or gRPC with TLS for Node.js clusters.

WebSocket has a separate job. It is a client transport for application queries, writes, and CDC subscriptions. It does not carry `ReplicationBatch` messages between Sirannon nodes. Production node-to-node replication uses `GrpcReplicationTransport`.

### Is it row-based, statement-based, operation-log based, or CRDT-like?

It is operation-log based at the Sirannon layer. A replicated change carries the table, operation, primary key, old data, new data, transaction ID, node ID, and HLC. It is not raw SQL statement replay, and the current production write path is not a CRDT protocol.

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

The current production path is primary-replica. Conflict resolvers decide how a receiving node applies a change to an existing row; they do not turn the replication engine into a multi-writer or CRDT system.

## Security

Sirannon-db is designed to be secure by default in its core operations:

- **Parameterised queries** - All SQL execution uses parameter binding through the driver layer, preventing SQL injection.
- **Identifier validation** - CDC table and column names are validated against a strict allowlist regex (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`).
- **Path traversal prevention** - Migration and backup paths reject null bytes, `..` segments, and control characters.
- **Request size limits** - HTTP bodies and WebSocket payloads are capped at 1 MB.

> **Warning:** The server accepts arbitrary SQL from clients. When you expose it beyond localhost, always use the `onRequest` hook to authenticate and authorise requests. See the [TypeScript package docs](packages/ts/README.md#security) for examples.
>
> The built-in server binds plain HTTP and WebSocket without TLS. When you serve traffic outside a trusted network, terminate TLS upstream with a reverse proxy (nginx, Caddy, a cloud load balancer) or your clients' bearer tokens and query payloads will travel in cleartext.

## Documentation

Full API reference, code examples, and configuration tables are in the [TypeScript package README](packages/ts/README.md).

## Benchmarks

The benchmark suite compares Sirannon's embedded SQLite performance against Postgres 17 across micro-operations, YCSB, TPC-C, and concurrency scaling. All benchmarks support driver switching via the `BENCH_DRIVER` environment variable. See [`packages/ts/benchmarks/BENCHMARKS.md`](packages/ts/benchmarks/BENCHMARKS.md) for setup, configuration, and statistical analysis methodology.

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
