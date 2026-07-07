# Sirannon DB - Node.js Example

This self-contained Node.js example demonstrates Sirannon DB core features with either the `better-sqlite3` driver or Node's built-in `node:sqlite` driver.

## Prerequisites

- Node.js >= 22
- pnpm

## Setup

From the monorepo root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

## Run

Use `better-sqlite3`:

```bash
cd packages/ts/examples/node
pnpm start
```

or explicitly:

```bash
pnpm run start:better-sqlite3
```

Use Node's built-in SQLite driver:

```bash
pnpm run start:node-native
```

The Node native script passes `--experimental-sqlite` before loading the example.

You can also choose the driver manually:

```bash
pnpm start -- --driver=better-sqlite3
node --experimental-sqlite --import tsx src/main.ts --driver=node
```

Supported driver values are `better-sqlite3`, `better`, `node`, and `node-native`.

## Features Demonstrated

- Driver selection with `better-sqlite3` or Node native SQLite
- Database creation via `Database.create()`
- Schema creation via `db.execute()`
- File-based migrations with `loadMigrations()` and `db.migrate()`
- Data insertion with `db.execute()`
- Queries with `db.query<T>()` and `db.queryOne<T>()`
- Transactions with `db.transaction(async tx => ...)`
- CDC subscriptions with `db.watch()` and `db.on().subscribe()`
- Connection pool configuration with `readPoolSize`
- Metrics via `Sirannon` with `metrics.onQueryComplete`
- Multi-tenant databases via `createTenantResolver()`
- Query hooks with `onBeforeQuery`, `onAfterQuery`, and `onDatabaseOpen`
- Backup with `db.backup()`
- Graceful shutdown with `db.close()` and `sirannon.shutdown()`
