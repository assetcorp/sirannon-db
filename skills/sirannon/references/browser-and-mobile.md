# The browser, Bun, React Native, and device sync

## A database in the browser

Install the WebAssembly build of SQLite that the browser driver loads:

```bash
pnpm add -E wa-sqlite
```

Open the database directly with the driver, with one read connection and WAL mode off, as the package's own browser example does. The driver stores the file in IndexedDB through `IDBBatchAtomicVFS` by default, and `vfs: 'AccessHandlePoolVFS'` selects wa-sqlite's file system for the origin private file system:

```ts
import { Database } from '@delali/sirannon-db'
import { waSqlite } from '@delali/sirannon-db/driver/wa-sqlite'

const db = await Database.create('app', '/app.db', waSqlite({ vfs: 'IDBBatchAtomicVFS' }), {
  readPoolSize: 1,
  walMode: false,
})
```

- Under Vite, add `optimizeDeps: { exclude: ['wa-sqlite'] }` to `vite.config.ts`, as the browser example does, so that Vite serves wa-sqlite's WebAssembly file from the package itself.
- A browser database takes no backups and loads no extensions, so the server holds the durable copy and device sync keeps the two in step.
- Load migrations with `migrationsFromFiles` from the core package, since the file loader needs `node:fs`. [migrations-and-tenants.md](migrations-and-tenants.md) shows the Vite form.

## Bun and React Native

Both drivers are experimental, so tell the user before you build on either one.

- On Bun, `bunSqlite()` from `@delali/sirannon-db/driver/bun` opens databases through `bun:sqlite` and needs no install.
- On React Native, install `expo-sqlite`, then open the database with `Database.create`, `expoSqlite()` from `@delali/sirannon-db/driver/expo`, and `readPoolSize: 1`. Expo takes no backups and loads no extensions either.

## Device sync

Device sync is experimental. A device writes to its own local database first, so it keeps working offline. A `SyncController` then pushes those writes to a server and pulls everyone else's changes over a WebSocket.

On the server, open the same database, watch every synced table, and turn device sync on. In releases after 0.3.3, the server refuses device sync with `DEVICE_SYNC_NOT_ACCEPTED` until you set `acceptDeviceSync: true`, and that option needs an `authenticate` hook:

```ts
await serverDb.watch('work_orders')

const server = createServer(sirannon, {
  acceptDeviceSync: true,
  authenticate: authenticateDevice,
})
```

On the device, migrate the local database and watch every synced table before the app writes to it, then start the controller:

```ts
import { toSubprotocolCredential } from '@delali/sirannon-db'
import { SyncController } from '@delali/sirannon-db/client'

await db.migrate(migrations)
await db.watch('work_orders')

const sync = new SyncController(db, {
  url: 'https://api.example.com',
  databaseId: 'app',
  tables: ['work_orders'],
  headers: { Authorization: `Bearer ${token}` },
  webSocketProtocols: [toSubprotocolCredential('app.device.', token)],
})

await sync.start()
```

- A write made before `db.watch` covers its table never reaches the server, even though `pendingPushCount` in `await sync.status()` reads zero.
- The live pull delivers only the changes that the server records after the device connects. A new device gets the rows already on the server through `await sync.downloadSnapshot()`, which works once `start()` resolves.
- Each side applies its own conflict resolver. The device applies the `resolver` in `SyncControllerOptions`, while the server applies the one in its execution target's `applyChanges`, so a `merge` resolver on the device alone leaves the two sides holding different rows.
- A live query on the device's local database updates when the controller commits a pulled change, so the interface can read from `db.live` alone.
- `await sync.stop()` closes the connection, and `onStatusChange` reports each change of state.

Read `device-sync.md`, `device-sync-recovery.md`, and `agent-offline.md` in the versioned documentation for snapshots, the migration handshake, and recovery.
