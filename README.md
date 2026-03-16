# sirannon-db

[![CI](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml/badge.svg)](https://github.com/assetcorp/sirannon-db/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![downloads](https://img.shields.io/npm/dw/@delali/sirannon-db)](https://www.npmjs.com/package/@delali/sirannon-db)
[![types](https://img.shields.io/badge/types-TypeScript-blue)](https://www.npmjs.com/package/@delali/sirannon-db)
[![license](https://img.shields.io/npm/l/@delali/sirannon-db)](https://github.com/assetcorp/sirannon-db/blob/main/LICENSE)

Turn any SQLite database into a networked data layer with real-time subscriptions. One library gives you connection pooling, change data capture, migrations, scheduled backups, and a client SDK that talks over HTTP or WebSocket.

> *sirannon* means 'gate-stream' in Sindarin.

## Install

```bash
pnpm add @delali/sirannon-db
```

Requires Node.js >= 22.

## Quick start

```ts
import { Sirannon } from '@delali/sirannon-db'

const sirannon = new Sirannon()
const db = sirannon.open('app', './data/app.db')

db.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)')
db.execute('INSERT INTO users (name, email) VALUES (?, ?)', ['Ada', 'ada@example.com'])

const users = db.query<{ id: number; name: string }>('SELECT * FROM users')
```

## Three entry points

The package ships three independent exports so you only bundle what you need:

| Import | What you get |
| --- | --- |
| `@delali/sirannon-db` | Core library: queries, transactions, CDC, migrations, backups, hooks, metrics, lifecycle |
| `@delali/sirannon-db/server` | HTTP + WebSocket server powered by uWebSockets.js |
| `@delali/sirannon-db/client` | Browser/Node.js client SDK with auto-reconnect and subscription restore |

## Features

- **Queries and transactions** - Execute reads, writes, and batch operations with automatic statement caching (LRU, capacity 128). Transactions provide full ACID guarantees.
- **Connection pooling** - 1 dedicated write connection + N read connections (default 4). WAL mode enabled by default for concurrent reads during writes.
- **Change data capture (CDC)** - Watch tables for INSERT, UPDATE, and DELETE events in real time through SQLite triggers and configurable polling.
- **Migrations** - File-based (numbered `.up.sql` / `.down.sql`) or programmatic migrations, tracked in a `_sirannon_migrations` table. Supports rollback to any version.
- **Backups** - One-shot snapshots via `VACUUM INTO` and scheduled backups on a cron expression with automatic file rotation.
- **Hooks** - Before/after hooks for queries, connections, and subscriptions. Throwing from a before-hook denies the operation.
- **Metrics** - Plug in callbacks to collect query timing, connection events, and CDC activity.
- **Lifecycle management** - Auto-open databases on first access with idle timeouts and LRU eviction for multi-tenant setups.
- **Server** - Expose any `Sirannon` instance over HTTP and WebSocket with a single function call. Includes health endpoints, CORS configuration, and an `onRequest` hook for authentication.
- **Client SDK** - Async API mirroring the core `Database` interface. Supports HTTP and WebSocket transports with automatic reconnection and subscription restoration.

## Security

Sirannon-db is designed to be secure by default in its core operations:

- **Parameterized queries** - All SQL execution uses parameter binding through better-sqlite3, preventing SQL injection.
- **Identifier validation** - CDC table and column names are validated against a strict allowlist regex (`/^[a-zA-Z_][a-zA-Z0-9_]*$/`).
- **Path traversal prevention** - Migration and backup paths reject null bytes, `..` segments, and control characters.
- **Request size limits** - HTTP bodies and WebSocket payloads are capped at 1 MB.
- **No secrets in code** - The library reads credentials from environment variables and never hardcodes tokens.

> **Warning:** The server accepts arbitrary SQL from clients. When you expose it beyond localhost, always use the `onRequest` hook to authenticate and authorize requests. See the [TypeScript package docs](packages/ts/README.md#server) for examples.
>
> The built-in server binds plain HTTP and WebSocket without TLS. When you serve traffic outside a trusted network, terminate TLS upstream with a reverse proxy (nginx, Caddy, a cloud load balancer) or your clients' bearer tokens and query payloads will travel in cleartext.

## Documentation

Full API reference, code examples, and configuration tables are in the [TypeScript package README](packages/ts/README.md).

## Benchmarks

The benchmark suite compares Sirannon's embedded SQLite performance against Postgres 17 across micro-operations, YCSB, TPC-C, and concurrency scaling. See [`packages/ts/benchmarks/BENCHMARKS.md`](packages/ts/benchmarks/BENCHMARKS.md) for setup instructions, configuration, Docker-based fair comparisons, and statistical analysis methodology.

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
