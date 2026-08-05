# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Build a networked SQLite service with connection pooling, change data capture, live queries, migrations, backups, and a client SDK. Applications reach Sirannon over HTTP or WebSocket, Sirannon nodes replicate primary-owned changes over gRPC, and end-user devices sync a whole local database offline-first through the same server.

Read the [documentation](https://sirannon.sondelali.com/docs), or run the [distributed entitlements example](packages/ts/examples/distributed-entitlements/) to watch a three-node cluster serve through a primary failure on your own machine.

> *sirannon* means 'gate-stream' in Sindarin.

## Project status

| Part | Status | Details |
| --- | --- | --- |
| Core engine ([`@delali/sirannon-db`](packages/ts/)) | Stable | Queries, transactions, connection pooling, change data capture, live queries, migrations, backups, hooks, metrics, and multi-tenant lifecycle, covered by more than 130 test files on Node 22 and 24. |
| Server and client (`@delali/sirannon-db/server`, `/client`) | Stable | HTTP and WebSocket access with reconnection and subscription restore. The server serves registered operations and accepts no SQL until you turn it on. |
| Device sync (`@delali/sirannon-db/client`) | Experimental | Offline-first two-way sync between a device's local database and a server, with push, live pull, snapshot resync, and a migration handshake. It is new and not yet proven in production. |
| Primary-replica replication (`@delali/sirannon-db/replication`) | Stable | Hybrid Logical Clock stamping, conflict resolvers, first sync, write concerns, and a gRPC transport with mutual TLS. |
| Coordinator-backed failover (`/replication/coordinator/etcd`) | Experimental | etcd authority, primary terms, and in-sync sets, verified by a Docker conformance run under fault injection. It is new and not yet proven in production. |
| Drivers | Stable: better-sqlite3, Node, wa-sqlite. Experimental: Bun, Expo | The Bun and Expo drivers run today but carry no TypeScript declarations yet. |

Durability follows SQLite's WAL mode with `synchronous=NORMAL` by default, and you can raise it. The [roadmap](ROADMAP.md) sets out what comes next.

## Install

```bash
pnpm add -E @delali/sirannon-db better-sqlite3
```

Pick the [driver](#pluggable-drivers) for your runtime; `better-sqlite3` is the usual choice on Node.js.

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
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, live queries, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/driver/*` | SQLite driver adapters |
| `@delali/sirannon-db/file-migrations` | Load `.up.sql` and `.down.sql` files from a directory |
| `@delali/sirannon-db/backup-scheduler` | Cron-scheduled backup runner with file rotation, also re-exported from the core entry |
| `@delali/sirannon-db/server` | HTTP and WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser and Node.js client SDK with auto-reconnect, subscription restore, and the device sync controller |
| `@delali/sirannon-db/client/topology` | Topology-aware client that routes reads and writes across a replication group |
| `@delali/sirannon-db/react` | `useLiveQuery` and `useCommand` hooks |
| `@delali/sirannon-db/codegen` | Typed operation references generated from your server's registry |
| `@delali/sirannon-db/replication` | Replication engine, primary-replica topology, HLC, write concerns, and conflict resolvers |
| `@delali/sirannon-db/replication/coordinator/etcd` | etcd-backed cluster coordinator for primary authority and automatic failover |
| `@delali/sirannon-db/transport/grpc` | gRPC replication transport with TLS support |
| `@delali/sirannon-db/transport/memory` | In-memory replication transport for tests and single-process clusters |

## Features

- **Queries and transactions.** Reads, writes, batches, and transactions run with full ACID guarantees over one write connection and a pool of read connections, with WAL mode on by default.
- **Change data capture.** Watch a table for insert, update, and delete events in real time through SQLite triggers and configurable polling.
- **Live queries.** `db.live` keeps a query result current by applying each change to the rows it already holds, and `@delali/sirannon-db/react` renders one through `useLiveQuery`.
- **Registered operations.** The server runs statements you registered under a name and accepts no SQL from the network by default. `sirannon-codegen` turns that registry into typed client references.
- **Migrations.** File-based or programmatic migrations apply once each with content checksums, mirror `PRAGMA user_version`, roll back to any version, squash into a baseline, and survive two processes migrating at once. A set declared on the registry covers every database it opens, tenants included.
- **Bulk load.** A large import runs in one transaction under relaxed durability, then Sirannon restores the configured level, so the import pays one durability barrier rather than one per row.
- **Backups.** Take a one-shot snapshot with `VACUUM INTO`, or schedule rotating backups on a cron expression.
- **Hooks and metrics.** Before and after hooks cover queries, connections, and subscriptions, and throwing from a before-hook denies the operation. Metrics callbacks collect query timing, connection events, and CDC activity.
- **Multi-tenant lifecycle.** Databases open on first access, close on an idle timeout, and evict least-recently-used past a cap.
- **Server and client SDK.** Expose a registry over HTTP and WebSocket with one call, and reach it through a client that mirrors the core interface, reconnects, and restores its subscriptions.
- **Device sync.** An end-user device keeps its whole local database in step with a server, offline-first and both ways, with snapshot resync, a migration handshake, and capability negotiation.
- **Distributed replication.** A primary stamps each change with a Hybrid Logical Clock and replicates checksummed batches to read replicas over gRPC with mutual TLS.
- **Coordinator-backed failover.** etcd authority, primary terms, in-sync sets, and write concerns keep write ownership clear, and a minority partition fails closed.
- **Conflict resolution.** Choose LWW, PrimaryWins, FieldMerge, or your own resolver for an incoming change that targets an existing row.

## Documentation

| Guide | What it covers |
| --- | --- |
| [Core engine](docs/core.md) | Bulk load, live queries, migrations, backups, hooks, metrics, and the multi-tenant lifecycle |
| [Server](docs/server.md) | HTTP routes, WebSocket messages, authentication, write shapes, the writer worker, and value encoding |
| [Registered operations](docs/operations.md) | Naming the statements a server runs, identity-filled arguments, capabilities, and code generation |
| [Live queries](docs/live-queries.md) | Maintained query results locally, over the network, and in React |
| [Client SDK](docs/client.md) | Transports, subscriptions, topology-aware routing, and read concern |
| [Device sync](docs/device-sync.md) | Offline-first two-way sync between a device's local database and a server |
| [Distributed replication](docs/replication.md) | Replication, first sync, write and read concerns, coordinator failover, resolvers, and transports |
| [Configuration reference](docs/configuration.md) | Every option table, from `SirannonOptions` to `GrpcReplicationOptions` |
| [Errors](docs/errors.md) | Every code, when it happens, whether the call is safe to retry, and its HTTP status |

The [specification](packages/spec/) defines the wire formats, value encodings, and replication invariants every implementation follows, and [`docs/adr/`](docs/adr/) holds the decision records behind the replication design.

## Examples

| Example | Runtime | What it demonstrates |
| --- | --- | --- |
| [`node`](packages/ts/examples/node/) | Node.js >= 22 | Core features, live queries, and multi-tenant lifecycle on either `better-sqlite3` or Node's built-in SQLite driver |
| [`web-wa-sqlite`](packages/ts/examples/web-wa-sqlite/) | Browser and Node.js | Offline-first device sync: a local database in the browser, snapshot load, offline writes, conflict resolution, and a local live query |
| [`web-client`](packages/ts/examples/web-client/) | Browser and Node.js | Live queries and the React hooks over registered operations, with no SQL on the wire |
| [`distributed-entitlements`](packages/ts/examples/distributed-entitlements/) | Node.js and browser | Three-node coordinator-backed replication with etcd, gRPC, mTLS, and Toxiproxy failure controls |

```bash
pnpm install && pnpm --filter @delali/sirannon-db build
cd packages/ts/examples/node && pnpm start
```

## Architecture

Application clients reach the primary and read replicas over HTTP and WebSocket. The primary accepts every write, assigns each change a Hybrid Logical Clock timestamp, and sends checksummed batches to the replicas over gRPC with mutual TLS. An etcd coordinator tracks primary authority, node leases, and the in-sync set, and promotes an in-sync replica when the primary fails.

<p align="center">
  <img src="docs/assets/replication-topology.svg" alt="Sirannon replication topology: application clients reach the primary and read replicas, the primary replicates to replicas over gRPC with mutual TLS, and an etcd coordinator tracks authority, leases, and the in-sync set." width="820">
</p>

## Security

- The server serves [registered operations](docs/operations.md) and accepts no SQL from the network until you set `acceptSql: true`. Authenticate every request either way through the `authenticate` hook, and check the `Origin` header on the WebSocket upgrade.
- A Node client sends its `headers` on the WebSocket upgrade, so the hook reads `headers.authorization` on both transports. A browser sends no handshake header, so a browser client carries a short-lived ticket in `webSocketProtocols`; the server selects the plain `sirannon.v1` identifier and never echoes the ticket. A refused upgrade closes with 4401 or 4403; the client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed.
- Every statement binds its parameters through the driver, so user input never reaches the SQL text.
- Sirannon validates CDC table and column names against `/^[a-zA-Z_][a-zA-Z0-9_]*$/`, and rejects null bytes, `..` segments, and control characters in migration and backup paths.
- HTTP bodies and WebSocket messages are capped at 1 MB, which `maxBodyBytes` raises or lowers.
- The built-in server binds plain HTTP and WebSocket. Terminate TLS upstream with a reverse proxy such as nginx or Caddy, or a cloud load balancer, before you carry traffic outside a trusted network.

## Benchmarks

The suite compares Sirannon against Postgres 17 on the same OLTP workloads: point-select, single-row-insert, single-row-update, YCSB A/B/C/F, and a TPC-C-shaped mix. It drives Sirannon over HTTP into its real server and Postgres over its socket, both as native processes on pinned cores under a hard memory ceiling at matched durability, under an open-loop load generator that corrects for coordinated omission. It also records change-feed latency, cold start, and connection scaling for Sirannon alone. The harness is a Python project under [`benchmarks/server`](benchmarks/server), and the write-up generator rewrites [`BENCHMARKS.md`](BENCHMARKS.md) from the latest committed run.

## Development

```bash
pnpm install
pnpm build
pnpm test
pnpm typecheck
pnpm lint
```

[`CONTRIBUTING.md`](CONTRIBUTING.md) covers the repository layout, the end-to-end and failover suites, and how to propose a change.

## License

Apache-2.0
