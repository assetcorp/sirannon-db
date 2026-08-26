# Sirannon Field Service Demo

A work order app whose data is a real SQLite database in the browser. Every read and write hits that local database through wa-sqlite and IndexedDB, so the page keeps working with the server switched off. A `SyncController` carries local writes up to the server and applies the server's changes back down, and the board on screen is a local live query that redraws when either happens. A SQL console in the page runs statements against that same local database, and `--mode browser-only` builds the whole app without device sync, which leaves a static site that needs no server behind it. The app is React on TanStack Start in SPA mode, styled with Tailwind and the shadcn primitives from `@delali/sirannon-example-shared`.

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

Open `http://localhost:5173`.

Or run the app on its own, with no server and no device sync:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run app:dev:browser
```

## Browser-only mode

`--mode browser-only` builds the app without device sync. The `app:dev:browser` and `build:browser` scripts pass that flag, and `vite.config.ts` turns it into the `__SIRANNON_BROWSER_ONLY__` constant, which the bundler replaces with `true` or `false` while it builds.

That constant guards the single dynamic import of [`src/lib/device-sync.ts`](src/lib/device-sync.ts), which is the only file that names `SyncController`. A browser-only build therefore carries no sync client at all, and you can check that for yourself:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run build:browser
grep -rl SyncController dist/client
```

The search finds nothing. Run `build` in place of `build:browser` and the same search finds the sync chunk.

The header drops the sync switch in this mode, and the page drops the status strip, the failure alert, and the snapshot panel, since none of them has anything to report without a server. Everything else stays, including the local database, the migration, the seed rows, the live query, and the SQL console. The output under `dist/client` is a directory of static files, so any static host can serve it.

## Devices

The first visit asks you to name the device, because the name decides which local database file the tab opens. The picker lists every device this browser already holds, from a registry the app keeps in localStorage, and `?device=<name>` in the URL stays the source of truth, so a bookmark reopens the same device. The name sits in the header the whole time, next to the control that switches to another device.

One device belongs to one tab. Each tab takes a Web Lock on its device name, so a second tab on the same name is refused with an explanation and a picker. Without that lock, two tabs on one name would share a single database file, and an edit appearing in the other tab would look like sync while being one file in two windows.

## What to try

1. **Claim a work order.** The card moves to `In progress` immediately, because the write went to the local database. Watch `Queued to push` go to 1 and back to 0 as the push loop drains.
2. **Open a second device.** Use the device control in the header, or add `?device=van-2` in a second tab. A device that has never synced pushes what it holds, then downloads a snapshot of the whole database, so it starts with everything the first device already has.
3. **Claim something on one device and watch the other.** `Changes from server` climbs and the board redraws with no reload. That change arrived over the pull socket, was applied to the local database, and the local live query picked it up from there.
4. **Turn sync off on both devices and edit the same order.** The switch in the header pauses the controller; local writes keep working and queue up. Turn both back on and the two edits converge: the later write wins on the hybrid logical clock, and all three copies agree.
5. **Stop the server and reload the page.** The app still opens with all its data, and new work orders still save. The status strip reads `Offline, working locally` and the alert shows what failed. Start the server again and the app reconnects on its own retry, sending the queued writes up.
6. **Open a new device with the server stopped.** The device seeds itself with the four fixed work orders, so the board is never empty, and every write queues. Start the server and the device pushes its queue, then takes its first snapshot.

7. **Open the SQL console.** Press the `SQL` button in the header, run `SELECT * FROM work_orders`, then insert a row and watch the board pick it up.

## The SQL console

The `SQL` button in the header opens a console across the bottom of the page. Type a statement, press Ctrl+Enter or Cmd+Enter, and the result appears below the editor: a grid of rows for a read, a count of changed rows for a write, and the message SQLite returned when it refuses the statement. Arrow Up and Arrow Down walk back through what you have already run.

The console holds the same local database the board reads, so a row you insert there appears on the board immediately through the live query, and it queues for push like any other local write. It runs one statement per press, because `Database` takes one statement per call. `SELECT`, `EXPLAIN`, `PRAGMA`, and a read-only `WITH` go to `db.query`, and everything else goes to `db.execute`, which is what keeps a write inside the write gate.

Run `SELECT * FROM _sirannon_meta` to watch the guard turn it down. Sirannon reserves every identifier beginning with `_sirannon`, and the public query API answers with an error in place of the row.

## How the two halves fit

[`src/schema.ts`](src/schema.ts) holds the migration both sides run, plus the fixed seed rows. The server registers the migration on the `Sirannon` registry, which is what lets the server hand the migration SQL to a device that is behind. The browser applies the same array locally, so a device that has never reached the server still has its tables.

[`src/data-server.ts`](src/data-server.ts) opens the database, watches `work_orders`, seeds the four orders on first run, and starts the server. It leaves `acceptSql` at its default, so this server runs no SQL from the network at all; the device sync routes are a separate surface and stay open. Check it with `curl http://localhost:9876/capabilities`. The file cannot be called `server.ts`, because TanStack Start treats `src/server.ts` as its own server entry.

[`src/lib/field-device.ts`](src/lib/field-device.ts) opens the local database, applies the migration, watches the table so that local writes reach the outbox, seeds a never-synced device, and builds the controller. [`src/features/field-service/use-field-device.ts`](src/features/field-service/use-field-device.ts) owns the lifecycle: it takes the tab lock, opens the device, starts sync, and closes everything when the device changes.

## The first sync

A device that has never synced seeds itself from `SEED_WORK_ORDERS` at open, so the app works with no server running. When the controller reaches the server and reports every local change pushed, the app downloads the first snapshot:

```ts
if (device.neverSynced) {
  watchForFirstSnapshot(store, device)
}
```

The order matters. The push must finish first, because a snapshot replaces the whole local database, and pushing first means nothing local is lost. The snapshot must still run, because a device with no pull cursor subscribes at the server's current position and would miss everything written before it joined.

Two devices seeded offline converge once both sync. The seed rows carry fixed ids and a fixed `updated_at`, so both devices push byte-identical rows, and whichever version the server keeps, every copy holds the same values. Work order ids for new rows are `crypto.randomUUID()` rather than `AUTOINCREMENT`, because two devices creating rows offline would hand out the same integers and collide the moment they both push.

## What the live query does here

```ts
useLiveQuery<WorkOrder>(device.liveDb, WORK_ORDERS_QUERY)
```

This runs against the local database, not the server. The controller applies pulled changes into that database inside a transaction, the change tracker picks them up, and the live query updates its rows. Nothing in the app applies a change event by hand. The hooks come from `@delali/sirannon-db/react`, and the local `Database` satisfies their `LiveDatabase` parameter, which `field-device.ts` proves with a typed assignment.

A snapshot drops and recreates the table, so the app unmounts the board while a snapshot runs, which closes the live query, and mounts it again once `onSnapshotComplete` reports the database usable.

The status strip consumes the controller's `onStatusChange` callback; nothing polls. The controller reports a status when it changes state, pushes a batch, applies a pulled batch, needs a resync, or records or clears an error.

## Browser limitations

These need a filesystem and stay on the server side:

- File-based migrations, since `loadMigrations()` uses `node:fs`
- Extensions, since wa-sqlite has no `load_extension`
- `db.backup()`
- `createTenantResolver()`

## Security model

This example binds to localhost, and it names the caller on every request. Three things are worth knowing before you copy it.

The server restricts CORS to the app origin and refuses SQL from the network, so a caller reaches the sync routes and nothing else. That part transfers.

Every request carries a credential, because a device needs both forms. The `headers` option covers the HTTP push and the snapshot download, and `webSocketProtocols` covers the pull subscription, which a browser opens with no header of its own. `createDeviceAuthenticator` in `src/device-identity.ts` reads whichever form the request carries, checks the `Origin`, and refuses anything else.

Replace the token before you deploy this, because this one is a shared constant that the browser bundle carries in the clear. A deployed fleet mints a short-lived ticket per device from a route the application owns, serves the whole thing over TLS, and redacts both the authorization header and the offered subprotocols from its access logs.

The work order table bounds what a device may write. Each text column carries a `CHECK` constraint, so the server enforces the same limit it enforces locally and a hand-written push can store no more than the form allows.

## Environment

```bash
SIRANNON_PORT=9876
HOST=127.0.0.1
APP_ORIGIN=http://localhost:5173
SIRANNON_DEVICE_TOKEN=sirannon-field-service-token
VITE_SIRANNON_URL=http://127.0.0.1:9876
VITE_SIRANNON_DEVICE_TOKEN=sirannon-field-service-token
```

The server reads `SIRANNON_DEVICE_TOKEN` and the browser reads `VITE_SIRANNON_DEVICE_TOKEN`, so set both to the same value or leave both unset. A browser-only build reads none of the `VITE_` variables, because it opens no connection.

The server database is stored in `data/`, which is ignored by git. Delete that directory to start over, and clear the site's IndexedDB storage to reset every device in a browser.
