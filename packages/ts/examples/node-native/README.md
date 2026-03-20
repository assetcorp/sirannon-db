# Sirannon DB - Node.js Built-in SQLite Example

Self-contained example demonstrating all Sirannon DB features with Node 22's built-in `node:sqlite` driver.

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

```bash
cd packages/ts/examples/node-native
pnpm start
```

The start script passes `--experimental-sqlite` to enable Node's built-in SQLite module.

## Features demonstrated

- Driver setup with `nodeSqlite()`
- Database creation via `Database.create()`
- Schema creation via `db.execute()`
- File-based migrations with `loadMigrations()` + `db.migrate()`
- Data insertion with `db.execute()`
- Queries with `db.query<T>()` and `db.queryOne<T>()`
- Transactions with `db.transaction(async tx => ...)`
- CDC subscriptions with `db.watch()` and `db.on().subscribe()`
- Connection pool configuration (`readPoolSize`)
- Metrics via `Sirannon` with `metrics.onQueryComplete`
- Multi-tenant databases via `createTenantResolver()`
- Query hooks (`onBeforeQuery`, `onAfterQuery`, `onDatabaseOpen`)
- Backup with `db.backup()`
- Graceful shutdown with `db.close()` and `sirannon.shutdown()`
