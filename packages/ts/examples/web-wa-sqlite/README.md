# Sirannon Field Service Demo

This example is a work order app that keeps its data in a SQLite database inside the browser. Every read and write goes to that local database through wa-sqlite and IndexedDB, so the page keeps working with the server switched off. A `SyncController` pushes local writes to the server and applies the server's changes to the local database. The board on screen is a local live query, which redraws after either kind of change commits to the local database.

A SQL console in the page executes statements against that same local database. Building with `--mode browser-only` leaves device sync out of the app, which produces a static site that needs no server behind it. The app uses React on TanStack Start in SPA mode, with Tailwind and the shadcn primitives from `@delali/sirannon-example-shared` for styling.

## Setup

This example needs Node.js 22 or newer and pnpm.

The server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

Build again whenever you change anything under `packages/ts/src`.

## Run

Start the server and the browser app together:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run dev
```

Or start them separately:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run server
pnpm --dir packages/ts/examples/web-wa-sqlite run app:dev
```

Open `http://localhost:5173`.

Or start the app on its own, with no server and no device sync:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run app:dev:browser
```

## Browser-only mode

`--mode browser-only` builds the app without device sync. The `app:dev:browser` and `build:browser` scripts pass that flag, and `vite.config.ts` turns it into the `__SIRANNON_BROWSER_ONLY__` constant, which the bundler replaces with `true` or `false` while it builds.

That constant guards the single dynamic import of [`src/lib/device-sync.ts`](src/lib/device-sync.ts), which is the only file that names `SyncController`. A browser-only build therefore contains no sync client at all, which you can check for yourself:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run build:browser
grep -rl SyncController dist/client
```

After a browser-only build, the search finds nothing. After `build` in place of `build:browser`, the same search finds the sync chunk.

In this mode, the app leaves the sync switch out of the header. It also leaves out the status strip, the failure alert, and the snapshot panel, since none of them has anything to report without a server. Everything else stays, including the local database, the migration, the seed rows, the live query, and the SQL console. The output under `dist/client` is a directory of static files, so any static host can serve it.

## Devices

On your first visit, the app asks you to name the device, because the name selects the local database file that the tab opens. The picker lists every device that this browser already holds, from a registry that the app keeps in localStorage. The `?device=<name>` parameter in the URL stays the source of truth, so a bookmark reopens the same device. The header shows the device name the whole time, next to the control that switches to another device.

Only one tab at a time can open a device. Each tab takes a Web Lock on its device name, so the app refuses a second tab on the same name and shows that tab an explanation and a picker. Without that lock, two tabs on one name would share a single database file, so an edit in one tab would show up in the other through that shared file and look like sync.

## What to try

1. **Claim a work order.** The card moves to `In progress` at once, because the write goes to the local database. Watch `Queued to push` go to 1 and back to 0 as the push loop drains the outbox.
2. **Open a second device.** Use the device control in the header, or add `?device=van-2` in a second tab. A device that has never synced pushes the rows that it holds and then downloads a snapshot of the whole database, so it starts with everything that the first device already has.
3. **Claim something on one device and watch the other.** `Changes from server` climbs and the board redraws with no reload. The controller receives that change over the pull socket and applies it to the local database, where the local live query picks it up.
4. **Turn sync off on both devices and edit the same order.** The switch in the header pauses the controller, while local writes keep working and queue up. Once you turn both back on, the later write wins on the hybrid logical clock, and all three copies converge on it.
5. **Stop the server and reload the page.** The app still opens with all its data, and it still saves new work orders. The status strip reads `Offline, working locally`, and the alert shows what failed. Once you start the server again, the app reconnects on its next retry and pushes the queued writes.
6. **Open a new device with the server stopped.** The device seeds itself with the four fixed work orders so that the board is never empty, and it queues every write. Once the server starts, the device pushes its queue and then downloads its first snapshot.
7. **Open the SQL console.** Press the `SQL` button in the header, execute `SELECT * FROM work_orders`, then insert a row and watch the board pick it up.

## The SQL console

The `SQL` button in the header opens a console across the bottom of the page. When you type a statement and press Ctrl+Enter or Cmd+Enter, the console shows the result below the editor: a grid of rows for a read, a count of changed rows for a write, and the message that SQLite returns when it refuses the statement. Arrow Up and Arrow Down step back through the statements that you have already executed.

The console works on the same local database that the board reads, so a row that you insert there appears on the board at once through the live query. That row also queues for push like any other local write. The console executes one statement per press, because `Database` takes one statement per call. It sends `SELECT`, `EXPLAIN`, `PRAGMA`, and a read-only `WITH` to `db.query` and everything else to `db.execute`, which keeps each write inside the write gate.

Execute `SELECT * FROM _sirannon_meta` to see the guard refuse it. Sirannon reserves every identifier beginning with `_sirannon`, so the public query API answers with an error in place of the row.

## How the two halves fit

[`src/schema.ts`](src/schema.ts) holds the migration that both sides apply, along with the fixed seed rows. The server registers the migration on the `Sirannon` registry, which lets the server send the migration SQL to a device that is behind. The browser applies the same array locally, so a device that has never connected to the server still has its tables.

[`src/data-server.ts`](src/data-server.ts) opens the database, watches `work_orders`, seeds the four orders the first time that it starts, and starts the server. It leaves `acceptSql` at its default, so this server executes no SQL from the network at all. It sets `acceptDeviceSync: true`, which opens the device sync routes, together with the `authenticate` hook that the server requires for that option. An `onBeforePush` hook from [`src/device-identity.ts`](src/device-identity.ts) records the fleet that first pushes from each device and refuses a push from any other fleet after that. Check the capabilities that the server announces with `curl http://localhost:9876/capabilities`. Keep the file name as it is, because TanStack Start treats `src/server.ts` as its own server entry.

[`src/lib/field-device.ts`](src/lib/field-device.ts) opens the local database, applies the migration, watches the table so that local writes reach the outbox, seeds a never-synced device, and builds the controller. [`src/features/field-service/use-field-device.ts`](src/features/field-service/use-field-device.ts) manages the lifecycle: it takes the tab lock, opens the device, starts sync, and closes everything when you switch devices.

## The first sync

A device that has never synced seeds itself from `SEED_WORK_ORDERS` when it opens, so the app works while the server is stopped. When the controller reaches the server and reports every local change pushed, the app downloads the first snapshot:

```ts
if (device.neverSynced) {
  watchForFirstSnapshot(store, device)
}
```

The push must finish first, because a snapshot replaces the whole local database and would discard any local write that the server has not yet received. The app must still download the snapshot, because a device with no pull cursor subscribes at the server's current position and would miss every change that other devices wrote before it joined.

Two devices that seed themselves offline converge once both sync. The seed rows have fixed ids and a fixed `updated_at`, so both devices push byte-identical rows, which means that every copy holds the same values whichever version the server keeps. The app gives each new work order an id from `crypto.randomUUID()`, because with `AUTOINCREMENT`, two devices that create rows offline would assign the same integers, and those rows would collide as soon as both devices push.

## What the live query does here

```ts
useLiveQuery<WorkOrder>(device.liveDb, WORK_ORDERS_QUERY)
```

This query reads the local database, not the server. The controller applies pulled changes to that database inside a transaction, and the live query updates its rows once the change tracker records those changes. The app applies no change event by hand. The hooks come from `@delali/sirannon-db/react`, and a typed assignment in `field-device.ts` checks at compile time that the local `Database` satisfies their `LiveDatabase` parameter.

A snapshot drops and recreates the table, so the app unmounts the board, and the live query with it, while a snapshot is in progress. The app mounts the board again once `onSnapshotComplete` reports the database usable.

The status strip updates from the controller's `onStatusChange` callback, with no polling. The controller reports a status when it changes state, pushes a batch, applies a pulled batch, needs a resync, or records or clears an error.

## Browser limitations

These features need a filesystem or native code, so they work on the server side only:

- `loadMigrations()` reads migration files through `node:fs`.
- An extension needs `load_extension`, which wa-sqlite does not provide.
- `db.backup()` writes its copy to a file.
- `createTenantResolver()` maps each tenant to a file path.

## Security model

This example binds to localhost and identifies the caller on every request. Read the points below before you copy it.

The server restricts CORS to the app origin and refuses SQL from the network, so a caller can change data only through the sync routes. You can keep that part as it is in a deployment.

A device sends its credential in two forms, because a browser attaches a header to an HTTP request but attaches none to a WebSocket. The `headers` option covers the HTTP push and the snapshot download, and `webSocketProtocols` covers the pull subscription. `createDeviceAuthenticator` in `src/device-identity.ts` reads whichever form the request includes, checks the `Origin`, and refuses anything else.

Replace the token before you deploy this, because this one is a shared constant that the browser bundle includes in plain text. In a deployment, the application should mint a short-lived ticket for each device from a route of its own, serve everything over TLS, and redact both the authorization header and the offered subprotocols from its access logs.

The fleet check keeps its record of which fleet owns each device in memory, so a restart of the server clears that record. In a deployment, store that record in your own database.

The work order table limits what a device may write. Each text column has a `CHECK` constraint, so the server enforces the same limit as the local database, and a hand-written push can store no more than the form allows.

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

The server keeps its database in `data/`, which the example's `.gitignore` excludes. Delete that directory to start over, and clear the site's IndexedDB storage to reset every device in a browser.
