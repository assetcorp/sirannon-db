# Sirannon DB - Node.js Example

This self-contained Node.js example shows the core features of Sirannon DB with either the `better-sqlite3` driver or Node's built-in `node:sqlite` driver.

## Setup

This example needs Node.js 22 or newer and pnpm.

The example imports `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

Build the package again whenever you change anything under `packages/ts/src`.

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

The Node native script passes `--experimental-sqlite` to Node before it loads the example.

You can also choose the driver manually:

```bash
pnpm start -- --driver=better-sqlite3
node --experimental-sqlite --import tsx src/main.ts --driver=node
```

The `--driver` flag accepts `better-sqlite3`, `better`, `node`, and `node-native`.

## Features Demonstrated

- The example selects either `better-sqlite3` or Node's native SQLite driver.
- It opens each database through a `Sirannon` registry with `registry.open()`.
- It creates the schema with `db.execute()`.
- It applies file-based migrations with `loadMigrations()` and `db.migrate()`.
- It inserts data with `db.execute()`.
- It queries with `db.query<T>()` and `db.queryOne<T>()`.
- It groups writes in a transaction with `db.transaction(async tx => ...)`.
- It subscribes to changes with `db.watch()` and `db.on().subscribe()`.
- It opens a live query with `db.live()`, which keeps its result current as rows change.
- It sets the size of the read connection pool with `readPoolSize`.
- It collects metrics through `Sirannon` with `metrics.onQueryComplete`.
- It opens tenant databases on first access through `sirannon.resolve()` and `createTenantResolver()`, and `maxOpen` closes the least recently used tenant.
- It registers query hooks with `onBeforeQuery`, `onAfterQuery`, and `onDatabaseOpen`.
- It takes a backup with `db.backup()`.
- It shuts down cleanly with `db.close()` and `sirannon.shutdown()`.
