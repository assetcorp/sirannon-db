![Sirannon, an open source SQLite database library](https://raw.githubusercontent.com/assetcorp/sirannon-db/main/assets/banner.png)

# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

With Sirannon, you keep real SQLite underneath your application as it grows, so you can keep the SQL that you wrote against a file on your laptop when you serve that file over HTTP and WebSocket or replicate it from a primary to its read replicas. The wire formats, the value encodings, and the replication invariants that every implementation must follow are in the language-agnostic specification under [`packages/spec`](packages/spec/). The TypeScript package in this repository is the reference implementation of that specification.

Read the [documentation](https://sirannon.sondelali.com/docs), or start the [distributed entitlements example](packages/ts/examples/distributed-entitlements/) on your own machine to watch a three-node cluster keep answering requests after its primary fails.

> *sirannon* means 'gate-stream' in Sindarin.

## Project status

| Part | Status | Details |
| --- | --- | --- |
| Core engine ([`@delali/sirannon-db`](packages/ts/)) | Stable | With the core engine, you get queries, transactions, connection pooling, change data capture, live queries, migrations, backups, hooks, metrics, and a multi-tenant lifecycle. Continuous integration tests it on Node 22 and 24. |
| Server and client (`@delali/sirannon-db/server`, `/client`) | Stable | Applications reach a database over HTTP and WebSocket through the client, which reconnects after a dropped connection and restores its subscriptions. The server refuses SQL from the network until you set `acceptSql: true`. |
| Device sync (`@delali/sirannon-db/client`) | Experimental | A device writes to its own local database first, so it can go on writing while it's offline. The device sync controller then keeps that database in step with a server in both directions, through a push, a live pull, a snapshot resync, and a migration handshake. |
| Primary-replica replication (`@delali/sirannon-db/replication`) | Stable | A primary stamps each change with a Hybrid Logical Clock and sends it to its replicas over gRPC with mutual TLS. The same export also provides conflict resolvers, first sync, and write concerns. |
| Coordinator-backed failover (`/replication/coordinator/etcd`) | Experimental | etcd records write authority, primary terms, and the in-sync set. So far, a Docker conformance suite under fault injection is the whole of its evidence. |
| Drivers | Stable: better-sqlite3, Node, wa-sqlite. Experimental: Bun, Expo | The published package includes TypeScript declarations for all five drivers. |

Sirannon opens each database in SQLite's WAL mode with `synchronous=NORMAL` by default, although you can raise that durability level. Read the [roadmap](ROADMAP.md) for the next stages of work.

## Install

```bash
pnpm add -E @delali/sirannon-db better-sqlite3
```

You'll use `better-sqlite3` on Node.js in the quick start below, so install it alongside the core package. For any other runtime, pick a driver from the [driver table](#pluggable-drivers).

To serve databases over HTTP and WebSocket through `@delali/sirannon-db/server`, install uWebSockets.js as well. The npm registry has no package named `uWebSockets.js`, so install the tagged GitHub release v20.69.0, which is the version in Sirannon's own development dependencies:

```bash
pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"
```

When the process cannot load uWebSockets.js, `server.listen()` fails with code `SERVER_DEPENDENCY_MISSING` and a message that gives this install command.

## Quick start

```ts
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

const sirannon = new Sirannon({ driver: betterSqlite3() })
const db = await sirannon.open('app', './data/app.db')

await db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
await db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
```

## Pluggable drivers

| Driver | Import | Runtime |
| --- | --- | --- |
| better-sqlite3 | `@delali/sirannon-db/driver/better-sqlite3` | Node.js |
| Node built-in | `@delali/sirannon-db/driver/node` | Node.js >= 22 |
| wa-sqlite | `@delali/sirannon-db/driver/wa-sqlite` | Browser (IndexedDB persistence) |
| Bun | `@delali/sirannon-db/driver/bun` | Bun |
| Expo | `@delali/sirannon-db/driver/expo` | React Native |

## Package exports

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, live queries, migrations, backups, hooks, metrics, and the lifecycle manager |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters |
| `@delali/sirannon-db/file-migrations` | A loader for `.up.sql` and `.down.sql` files in a directory |
| `@delali/sirannon-db/backup` | Backup destination types, backup chain records, and `restoreBackup` |
| `@delali/sirannon-db/backup-scheduler` | A cron-scheduled backup runner with file rotation |
| `@delali/sirannon-db/server` | An HTTP and WebSocket server built on uWebSockets.js |
| `@delali/sirannon-db/client` | A client SDK for browsers and Node.js, with automatic reconnection, subscription restore, and the device sync controller |
| `@delali/sirannon-db/client/topology` | A topology-aware client that routes reads and writes across a replication group |
| `@delali/sirannon-db/react` | The `useLiveQuery` and `useCommand` hooks |
| `@delali/sirannon-db/codegen` | A generator of typed operation references from your server's registry |
| `@delali/sirannon-db/replication` | The replication engine, the primary-replica topology, HLC, write concerns, and conflict resolvers |
| `@delali/sirannon-db/replication/coordinator/etcd` | An etcd-backed cluster coordinator for primary authority and automatic failover |
| `@delali/sirannon-db/transport/grpc` | A gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | An in-memory replication transport for tests and single-process clusters |

## Features

- **Queries and transactions.** SQLite's ACID guarantees apply to every read, write, batch, and transaction. Sirannon sends every write through one connection, while on a driver that supports several connections it sends reads through a pool.
- **Change data capture.** SQLite triggers record every insert, update, and delete on a table that you watch. Sirannon polls that record at an interval that you can set, then delivers each new event to the table's subscribers.
- **Live queries.** `db.live` keeps a query result current by applying each change to the rows that it already holds. In React, the `useLiveQuery` hook from `@delali/sirannon-db/react` returns that result to your component.
- **Registered operations.** A caller invokes a statement that you registered under a name, while the SQL stays on the server. The `sirannon-codegen` command generates typed client references from that registry.
- **Migrations.** Sirannon applies each migration once, whether it comes from a file or from code, and records a checksum of its content. It mirrors the highest applied version into `PRAGMA user_version`, rolls back to any version, and squashes old history into a baseline. When two processes migrate one database at once, each migration still applies once. Sirannon also migrates every database that a registry opens, each tenant included, against the set that you declare on that registry.
- **Bulk load.** `bulkLoad` writes a large import in one transaction under relaxed durability and finishes with one checkpoint that syncs the file to disk. It restores the durability level that you configured once the load ends.
- **Backups.** `backup()` copies a database to a file while the database stays open for reads and writes, because SQLite copies the pages in steps and lets a write proceed between two steps. `scheduleBackup()` repeats that copy on a cron expression. `backupTo()` sends the copy to storage that you supply. With the `backups` option, Sirannon takes one full copy and then copies only the changes since each previous run, so `restoreBackup()` can rebuild the database at any moment between the full copy and the latest run.
- **Hooks and metrics.** Sirannon calls your hooks before and after each query, before each connection, subscription, and snapshot, and whenever it opens or closes a database. A before-hook that throws refuses the operation. Sirannon also reports query timings, connection events, and CDC activity to the metrics callbacks that you pass.
- **Multi-tenant lifecycle.** Sirannon opens a database on first access, closes it after an idle timeout, and evicts the least recently used one once the number of open databases reaches the cap that you set.
- **Server and client SDK.** `createServer` exposes a registry over HTTP and WebSocket. The client reconnects after a dropped connection and restores its subscriptions.
- **Device sync.** The device sync controller keeps an end-user device's whole local database in step with a server in both directions. It handles snapshot resync, the migration handshake, and capability negotiation. A device writes to its local database first, so it can go on writing while it's offline.
- **Distributed replication.** A primary stamps each change with a Hybrid Logical Clock and sends checksummed batches of changes to its read replicas over gRPC with mutual TLS.
- **Coordinator-backed failover.** An etcd coordinator records which node holds write authority in each primary term. A node that loses contact with the majority of its group refuses writes.
- **Conflict resolution.** When a node receives a change for a row that already exists there, Sirannon applies the conflict resolver that you choose, whether last-writer-wins, primary-wins, field merge, or one that you write.

## Documentation

| Guide | Topics |
| --- | --- |
| [Core engine](docs/core.md) | Bulk load, live queries, migrations, hooks, metrics, and the multi-tenant lifecycle |
| [Backups](docs/backups.md) | Copies to a file or to storage that you supply, the chain of changes after a full copy, and restoring from a moment that you name |
| [Server](docs/server.md) | HTTP routes, WebSocket messages, authentication, write shapes, the writer worker, and value encoding |
| [Registered operations](docs/operations.md) | Named statements, identity-filled arguments, capabilities, and code generation |
| [Live queries](docs/live-queries.md) | Maintained query results locally, over the network, and in React |
| [Client SDK](docs/client.md) | Transports, subscriptions, topology-aware routing, and read concern |
| [Device sync](docs/device-sync.md) | Offline-first two-way sync between a device's local database and a server |
| [Distributed replication](docs/replication.md) | Replication, first sync, write and read concerns, coordinator failover, resolvers, and transports |
| [Configuration reference](docs/configuration.md) | Every option table, from `SirannonOptions` to `GrpcReplicationOptions` |
| [Errors](docs/errors.md) | Every error code, the condition behind it, whether a retry is safe, and its HTTP status |

The wire formats, the value encodings, and the replication invariants are in the [specification](packages/spec/). The decision records behind the replication and backup designs are in [`docs/adr/`](docs/adr/).

## Examples

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](packages/ts/examples/node/) | Node.js >= 22 | Core features, live queries, and the multi-tenant lifecycle on either `better-sqlite3` or Node's built-in SQLite driver |
| [`web-wa-sqlite`](packages/ts/examples/web-wa-sqlite/) | Browser and Node.js | Offline-first device sync with a local database in the browser, snapshot load, offline writes, conflict resolution, and a local live query |
| [`web-client`](packages/ts/examples/web-client/) | Browser and Node.js | Live queries and the React hooks over registered operations, with no SQL in any request |
| [`distributed-entitlements`](packages/ts/examples/distributed-entitlements/) | Node.js and browser | Three-node coordinator-backed replication with etcd, gRPC, mTLS, and Toxiproxy failure controls |

Every example imports the built package, so build it from the repository root before you start one. To bring up the three-node cluster and its dashboard with the commands below, you need Docker with Compose and Node.js 22 or newer.

```bash
pnpm install && pnpm --filter @delali/sirannon-db build
cd packages/ts/examples/distributed-entitlements && pnpm run dev
```

You need only Node.js for the single-node example, so build the package as above and then enter `cd packages/ts/examples/node && pnpm start`.

## Architecture

Application clients reach the current primary and the eligible read replicas over HTTP and WebSocket. The primary accepts writes while it holds live write authority in etcd. It stamps each change with a Hybrid Logical Clock timestamp and sends checksummed batches of changes to the replicas over gRPC with mutual TLS. When failover is safe, a Sirannon failover controller picks an eligible in-sync replica from the leases and the group state in etcd, and it then advances the primary term in one atomic update. Every node refuses a write or a replication batch with a stale term.

<p align="center">
  <img src="docs/assets/replication-topology.svg" alt="Diagram of Sirannon's coordinator-backed replication. Clients write to the current primary and read from eligible nodes. The primary replicates to the replicas over gRPC with mutual TLS. A Sirannon controller performs failover through leases and atomic term updates in etcd." width="820">
</p>

## Security

- The server refuses SQL from the network until you set `acceptSql: true`, so give callers their reads and writes through [registered operations](docs/operations.md). Authenticate every request through the `authenticate` hook, and check the `Origin` header in that hook on each WebSocket upgrade.
- A Node client sends its `headers` on the WebSocket upgrade, so the hook reads `headers.authorization` on both transports. A browser sends no header with the handshake, so a browser client puts a short-lived ticket in `webSocketProtocols`. The server selects the plain `sirannon.v1` identifier, so the ticket stays out of the handshake response.
- When the hook refuses an upgrade, the server closes the connection with code 4401 or 4403, and the client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed.
- The driver binds each parameter separately from the SQL text, so SQLite treats a value that you pass as a parameter as data.
- Sirannon checks each CDC table and column name against `/^[a-zA-Z_][a-zA-Z0-9_]*$/`. It refuses a migration or backup path with a control character, a null byte included, or a `..` segment.
- The server caps each HTTP body and WebSocket message at 1 MB by default, and you can change that cap with `maxBodyBytes`.
- The built-in server listens on plain HTTP and WebSocket, so terminate TLS at a reverse proxy such as nginx or Caddy, or at a cloud load balancer, before any client outside your trusted network connects.

## Benchmarks

The benchmark harness measures Sirannon and Postgres 17 on the same OLTP workloads, which are point-select, single-row-insert, single-row-update, YCSB A, B, C, and F, and a TPC-C-shaped mix. It drives Sirannon through the SDK's WebSocket transport into the real server, while it drives Postgres through node-postgres on the Postgres binary socket protocol. Both engines work as native processes on pinned cores under a hard memory ceiling at matched durability. One open-loop load generator, which corrects for coordinated omission, drives both of them. The harness also records change-feed latency, cold start, and connection scaling for Sirannon alone.

You'll find the harness under [`benchmarks/server`](benchmarks/server), where a Node load generator drives the engines and a Python step joins their results. The write-up generator rewrites [`BENCHMARKS.md`](BENCHMARKS.md) from the latest committed run.

<!-- BENCH:headline START -->
On point-select at 10,000,000 rows, with both engines fsyncing every commit, Sirannon sustained 64.0K operations a second against PostgreSQL's 16.0K. At those operating points, Postgres had the lower p99 latency, 2.378 ms against Sirannon's 6.177 ms. Across all 8 workloads at this durability level, Postgres had the lower p99 latency on 7, so read each rate together with its latency. The harness recorded both engines in run `20260804T221053Z` on 2026-08-04, on GCP c3-standard-8-lssd, us-central1-b. You'll find every workload, both durability levels, and the full method in [`BENCHMARKS.md`](BENCHMARKS.md).
<!-- BENCH:headline END -->

## Development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for the repository layout, the end-to-end and failover suites, and how to propose a change.

## License

Apache-2.0
