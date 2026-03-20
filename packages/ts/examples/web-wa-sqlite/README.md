# Sirannon DB - Browser + wa-sqlite Example

Browser-based example running Sirannon DB with wa-sqlite and IndexedDB persistence via Vite.

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
cd packages/ts/examples/web-wa-sqlite
pnpm dev
```

Open the URL printed by Vite in your browser. The dev server sets `Cross-Origin-Opener-Policy` and `Cross-Origin-Embedder-Policy` headers required by `SharedArrayBuffer`.

## Features demonstrated

- Browser-compatible wa-sqlite driver with `IDBBatchAtomicVFS`
- Database creation in the browser via `Database.create()`
- Schema creation and inline migrations
- CRUD operations (insert, query)
- Transactions with `db.transaction(async tx => ...)`
- CDC subscriptions with `db.watch()` and `db.on().subscribe()`

## Browser limitations

The following features are Node.js-only and not included in this example:

- File-based migrations (`loadMigrations()` uses `node:fs`)
- Extensions (`wa-sqlite` does not support `load_extension`)
- Backup (`db.backup()` writes to the filesystem)
- Multi-tenant via `createTenantResolver()` (uses `node:fs`)
