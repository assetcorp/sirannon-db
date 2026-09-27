# Sirannon Field Service Demo

This example is a work order app that stores its data in a SQLite database inside the browser. The app sends every read and write to that local database through wa-sqlite and IndexedDB, so the page works with the server switched off. A `SyncController` pushes local writes to the server and applies the server's changes to the local database. The board on screen renders a local live query, so the board redraws after either kind of change commits to the local database.

A SQL console in the page executes statements against that same local database. When you build with `--mode browser-only`, the build omits device sync and produces a static site for any static host. The app is a React app on TanStack Start in SPA mode, styled with Tailwind and the shadcn primitives from `@delali/sirannon-example-shared`.

## Setup

To start this example, you need Node.js 22 or newer and pnpm.

The server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

Build again whenever you change anything under `packages/ts/src`.

## Start the example

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

With `--mode browser-only`, Vite builds the app without device sync. The `app:dev:browser` and `build:browser` scripts pass that flag. From the flag, `vite.config.ts` defines the `__SIRANNON_BROWSER_ONLY__` constant, which the bundler replaces with `true` or `false` during the build.

The app loads [`src/lib/device-sync.ts`](src/lib/device-sync.ts), the only file that imports `SyncController`, through one dynamic import inside a check on that constant. A browser-only build therefore contains no sync client, which you can check for yourself:

```bash
pnpm --dir packages/ts/examples/web-wa-sqlite run build:browser
grep -rl SyncController dist/client
```

After a browser-only build, `grep` prints no file. After `build` in place of `build:browser`, it prints the sync chunk.

In this mode, the header has no sync switch and no device switch. The page also has no status strip, failure alert, or snapshot panel. The local database, the migration, the seed rows, the live query, and the SQL console all work as they do in the full build. The output under `dist/client` is a directory of static files, so any static host can serve it.

## Devices

On your first visit, the page shows a form for the device name, because the app builds the file name of the local database from it. The picker lists every device in this browser, from a registry that the app stores in localStorage. The app reads the device from the `?device=<name>` parameter in the URL, so when you open a bookmarked URL, you get the same device. The header shows the device name at all times, next to the control that switches to another device.

Only one tab at a time can open a device. Each tab requests a Web Lock on its device name. When a second tab opens the same name, its lock request fails, so that tab displays an explanation and a picker in place of the board. Without that lock, two tabs on one name would share a single database file, so an edit in one tab would show up in the other through that shared file and look like sync.

In a browser-only build, however, each browser has one device, because without a server, a second device would have a separate board that never receives the work orders of the first. The app stores the name from the first visit in localStorage, so it opens the same local database on every later visit. In this mode, the app reads no `?device=` parameter. A second tab displays an explanation of the lock and a button that reloads the page, which you can press once you close the first tab.

## What to try

In a browser-only build, try steps 1 and 7, because steps 2 to 6 work only while the server is up. That build has no status strip, so the push count in step 1 is absent too.

1. **Claim a work order.** The card moves to `In progress` at once, because the app writes to the local database. Watch `Queued to push` go to 1 and back to 0 as the push loop drains the outbox.
2. **Open a second device.** Use the device control in the header, or add `?device=van-2` in a second tab. A new device pushes its rows and then downloads a snapshot of the whole database, so it starts with every row from the first device.
3. **Claim something on one device and watch the other.** `Changes from server` climbs and the board redraws with no reload. The controller receives that change over the pull socket and applies it to the local database, from which the local live query updates.
4. **Turn sync off on both devices and edit the same order.** The switch in the header pauses the controller, and the app queues every local write in the meantime. Once you turn both back on, the conflict resolver selects the write with the later hybrid logical clock timestamp, and all three copies converge on it.
5. **Stop the server and reload the page.** The app opens with all its data and saves new work orders. The status strip displays `Offline, working locally`, and the alert displays the error. Once you start the server again, the app reconnects on its next retry and pushes the queued writes.
6. **Open a new device with the server stopped.** The app seeds the new device with the four fixed work orders so that the board has rows from the start. It queues every write until the server is back, and then the device pushes its queue and downloads its first snapshot.
7. **Open the SQL console.** Press the `SQL` button in the header and execute `SELECT * FROM work_orders`. Then insert a row and watch it appear on the board.

## The SQL console

The `SQL` button in the header opens a console across the bottom of the page. When you type a statement and press Ctrl+Enter or Cmd+Enter, the console shows the result below the editor: a grid of rows for a read, a count of changed rows for a write, and the error message from SQLite when the statement fails. Press Arrow Up and Arrow Down to move through the statements that you executed earlier.

The console and the board use the same local database, so a row that you insert in the console appears on the board at once through the live query. The app also queues that row for push like any other local write. The console executes one statement per press, because each `Database` call executes one statement. It sends `SELECT`, `EXPLAIN`, `PRAGMA`, `VALUES`, and a `WITH` that contains no `INSERT`, `UPDATE`, `DELETE`, or `REPLACE` to `db.query`, and everything else to `db.execute`, so Sirannon executes every write inside the write gate.

Execute `SELECT * FROM _sirannon_meta` to see the result. Sirannon reserves every identifier that begins with `_sirannon`, so the public query API returns an error in place of the rows.

## How the two halves fit

[`src/schema.ts`](src/schema.ts) contains the migration that both sides apply, along with the fixed seed rows. The server registers the migration on the `Sirannon` registry, so that the server can send the migration SQL to a device with an older schema. The app applies the same array in the browser, so a device with no server connection yet still has its tables.

The code in [`src/data-server.ts`](src/data-server.ts) opens the database, watches `work_orders`, seeds the four orders on the first start, and starts the server. `acceptSql` stays at its default, so this server executes no SQL from the network. The code sets `acceptDeviceSync: true` to open the device sync routes, together with an `authenticate` hook, because the server throws `INVALID_DEVICE_SYNC` at construction when `acceptDeviceSync` has no `authenticate` hook. An `onBeforePush` hook from [`src/device-identity.ts`](src/device-identity.ts) stores the fleet of the first push from each device and throws `HookDeniedError` for a later push from any other fleet. List the capabilities of the server with `curl http://localhost:9876/capabilities`. Keep the file name as it is, because TanStack Start reserves `src/server.ts` for its own server entry.

The code in [`src/lib/field-device.ts`](src/lib/field-device.ts) opens the local database, applies the migration, watches the table so that Sirannon records local writes in the outbox, seeds a never-synced device, and builds the controller. The hook in [`src/features/field-service/use-field-device.ts`](src/features/field-service/use-field-device.ts) manages the lifecycle: it requests the tab lock, opens the device, starts sync, and closes everything when you switch devices.

## The first sync

When a device with no sync history opens, the app seeds it from `SEED_WORK_ORDERS`, so the board has rows while the server is stopped. When the controller connects to the server and its status shows no pending pushes, the app downloads the first snapshot:

```ts
if (sync !== null && device.neverSynced) {
  watchForFirstSnapshot(store, sync)
}
```

The push must finish first, because a snapshot replaces the whole local database and would discard any local write that the server does not yet have. The app must still download the snapshot, because a device with no pull cursor subscribes at the current position of the server and would miss every change that other devices write before it subscribes.

When two devices seed offline, their copies converge once both sync. The seed rows have fixed ids and a fixed `updated_at`, so both devices push byte-identical rows, which means that every copy holds the same values whichever version the server stores. The app gives each new work order an id from `crypto.randomUUID()`, because with `AUTOINCREMENT`, two devices that create rows offline would assign the same integers, and those rows would collide as soon as both devices push.

## The live query

```ts
useLiveQuery<WorkOrder>(device.liveDb, WORK_ORDERS_QUERY)
```

This query selects from the local database, not from the server. The controller applies pulled changes to that database inside a transaction, and Sirannon updates the rows of the live query once the change tracker records those changes. The app applies no change event by hand. The app imports the hooks from `@delali/sirannon-db/react`, and in `field-device.ts`, a typed assignment makes the compiler check that the local `Database` satisfies their `LiveDatabase` parameter.

A snapshot drops and recreates the table, so the app unmounts the board, and the live query with it, while a snapshot is in progress. The app mounts the board again once the controller calls `onSnapshotComplete` with a success, or with a failure for which the controller schedules no retry.

The status strip updates from the `onStatusChange` callback of the controller, with no polling. The controller calls `onStatusChange` when it changes state, pushes a batch, applies a pulled batch, marks a resync as required, or records or clears an error.

## Browser limitations

Each of these features uses a filesystem or native code, so it works only on the server:

- `loadMigrations()` reads migration files through `node:fs`.
- Loading an extension calls `load_extension`, which wa-sqlite does not include.
- `db.backup()` writes its copy to a file.
- `createTenantResolver()` maps each tenant to a file path.

## Security model

The server in this example binds to localhost and authenticates the caller on every request. Read the points below before you copy it.

The server limits CORS to the app origin and executes no SQL from the network, so a caller can change data only through the sync routes. You can keep that part as it is in a deployment.

A device sends its credential in two forms, because the browser WebSocket API sets no custom header on the upgrade request. The app passes the credential in `headers` for the HTTP push and the snapshot download, and in `webSocketProtocols` for the pull subscription. `createDeviceAuthenticator` in `src/device-identity.ts` checks the `Origin` header when a request has one, reads the credential from whichever form the request includes, and throws `RequestDeniedError` when either check fails.

Replace the token before you deploy this, because this one is a shared constant that the browser bundle includes in plain text. In a deployment, the application should mint a short-lived ticket for each device from a route of its own, serve everything over TLS, and redact both the authorization header and the offered subprotocols from its access logs.

The fleet check stores the fleet of each device in memory, so a restart of the server clears that record. In a deployment, store that record in your own database.

Each text column of the work order table has a `CHECK` constraint, so the server applies the same limits as the local database. A hand-written push can therefore store no more than the form can.

## Environment

```bash
SIRANNON_PORT=9876
HOST=127.0.0.1
APP_ORIGIN=http://localhost:5173
SIRANNON_DEVICE_TOKEN=sirannon-field-service-token
VITE_SIRANNON_URL=http://127.0.0.1:9876
VITE_SIRANNON_DEVICE_TOKEN=sirannon-field-service-token
```

The server reads `SIRANNON_DEVICE_TOKEN` and the browser reads `VITE_SIRANNON_DEVICE_TOKEN`, so set both to the same value or leave both unset. A browser-only build uses none of the `VITE_` variables, because the app opens no connection in that mode.

The server stores its database in `data/`, which the example's `.gitignore` excludes. Delete that directory to start over, and clear the site's IndexedDB storage to reset every device in a browser.
