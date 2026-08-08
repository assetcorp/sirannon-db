# Sirannon Field Service Demo

A work order app whose data is a real SQLite database in the browser. Every read and write hits that local database through wa-sqlite and IndexedDB, so the page keeps working with the server switched off. A `SyncController` carries local writes up to the server and applies the server's changes back down, and the list on screen is a local live query that redraws when either happens.

## Setup

This example needs Node.js 22 or newer and pnpm.

The server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you run anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

Run the build again whenever you change anything under `packages/ts/src`.

## Run

Start the server and the browser app together:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run dev
```

Or run them separately:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run server
pnpm --dir packages/ts/examples/web-wa-sqlite run app:dev
```

Open `http://localhost:5173`. That page is device `van-1`. Add `?device=van-2` in a second tab and you have two devices with two separate local databases, syncing through one server.

## What to try

1. **Claim a work order.** The card changes immediately, because the write went to the local database. Watch `Waiting to push` go to 1 and back to 0 as the push loop drains.
2. **Open the second tab.** A device with no history downloads a snapshot of the whole database, so `van-2` starts with everything `van-1` already has.
3. **Claim something on one device and watch the other.** `Changes from server` climbs and the list redraws with no reload. That change arrived over the pull socket, was applied to the local database, and the local live query picked it up from there.
4. **Take both devices offline and edit the same order.** The button pauses the controller; local writes keep working and queue up. Bring both back and the two edits converge: the later write wins on the hybrid logical clock, and all three copies agree.
5. **Stop the server and reload the page.** The app still opens with all its data. New work orders still save. `Sync state` reads `stopped` and the error line shows what failed. Start the server, press `Come online`, and the queued writes go up.

## How the two halves fit

[`src/schema.ts`](src/schema.ts) holds the migration both sides run. The server registers it on the `Sirannon` registry, which is what lets the server hand the migration SQL to a device that is behind. The browser applies the same array locally, so a device that has never reached the server still has its tables.

[`src/server.ts`](src/server.ts) opens the database, watches `work_orders`, seeds four orders on first run, and starts the server. It leaves `acceptSql` at its default, so this server runs no SQL from the network at all; the device sync routes are a separate surface and stay open. Check it with `curl http://localhost:9876/capabilities`.

[`src/device.ts`](src/device.ts) opens the local database, applies the migration, watches the table so local writes reach the outbox, then starts the controller:

```ts
await sync.start()
if (neverSynced) {
  await sync.downloadSnapshot()
}
```

Both calls matter. `start()` reconciles migrations and attaches to the change feed from the current moment, so on its own it leaves a fresh device with the right tables and no rows. `downloadSnapshot()` is what fills them. A device that has synced before skips it and resumes from its cursor.

Work order ids are `crypto.randomUUID()` rather than `AUTOINCREMENT`. Two devices creating rows offline would hand out the same integers and collide the moment they both push.

## What the live query does here

```ts
db.live<WorkOrder>('SELECT ... FROM work_orders ORDER BY site, task')
```

This runs against the local database, not the server. The controller applies pulled changes into that database inside a transaction, the change tracker picks them up, and the live query updates its rows. Nothing in [`src/main.ts`](src/main.ts) applies a change event by hand.

A snapshot drops and recreates the table, so the page closes the live query when `onResyncRequired` fires and reopens it once `onSnapshotComplete` reports the database usable again.

## Browser limitations

These need a filesystem and stay on the server side:

- File-based migrations, since `loadMigrations()` uses `node:fs`
- Extensions, since wa-sqlite has no `load_extension`
- `db.backup()`
- `createTenantResolver()`

The dev server sets `Cross-Origin-Opener-Policy` and `Cross-Origin-Embedder-Policy`, which wa-sqlite's `SharedArrayBuffer` build needs.

## Security model

This example binds to localhost and runs no authentication. Two things are worth knowing before you copy it.

The server restricts CORS to the app origin and refuses SQL from the network, so a caller reaches the sync routes and nothing else. That part transfers.

The authentication does not, because there is nothing to transfer. `SyncController` sends `headers` on its HTTP requests, but it opens the pull WebSocket with no credentials and offers no option to add any, so a browser device cannot authenticate that socket today. Put a similar deployment behind an authenticating proxy that terminates TLS and checks the upgrade, and treat this example as a localhost demonstration.

## Environment

```bash
SIRANNON_PORT=9876
HOST=127.0.0.1
APP_ORIGIN=http://localhost:5173
VITE_SIRANNON_URL=http://127.0.0.1:9876
```

The server database is stored in `data/`, which is ignored by git. Delete that directory to start over, and clear the site's IndexedDB storage to reset a device.
