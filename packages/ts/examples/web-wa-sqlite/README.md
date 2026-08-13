# Sirannon Field Service Demo

A work order app whose data is a real SQLite database in the browser. Every read and write hits that local database through wa-sqlite and IndexedDB, so the page keeps working with the server switched off. A `SyncController` carries local writes up to the server and applies the server's changes back down, and the board on screen is a local live query that redraws when either happens. The app is React on TanStack Start in SPA mode, styled with Tailwind and the shadcn primitives from `@delali/sirannon-example-shared`.

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

This example binds to localhost and runs no authentication. Two things are worth knowing before you copy it.

The server restricts CORS to the app origin and refuses SQL from the network, so a caller reaches the sync routes and nothing else. That part transfers.

The authentication does not, because this example configures none. A deployed device needs both credentials: `headers` covers the HTTP requests, and `webSocketProtocols` covers the pull WebSocket, which a browser opens without any header of its own. Mint a short-lived ticket per device rather than passing a long-lived token, check the `Origin` of the upgrade, serve the whole thing over TLS, and redact both the authorization header and the offered subprotocols from your access logs.

## Environment

```bash
SIRANNON_PORT=9876
HOST=127.0.0.1
APP_ORIGIN=http://localhost:5173
VITE_SIRANNON_URL=http://127.0.0.1:9876
```

The server database is stored in `data/`, which is ignored by git. Delete that directory to start over, and clear the site's IndexedDB storage to reset every device in a browser.
