# Browser and phone databases, and device sync

## A database on the device

- In the browser, install `wa-sqlite`, then open the database through the registry as on a server: `new Sirannon({ driver: waSqlite() })` and `await sirannon.open(id, path)`.
- Under Vite, add `optimizeDeps: { exclude: ['wa-sqlite'] }` to `vite.config.ts`.
- On React Native, install `expo-sqlite` and use `expoSqlite()`. Tell the user that the Expo driver is experimental.
- A device database takes no backups and loads no extensions, so the server keeps the durable copy.
- Build migrations with `migrationsFromFiles`, as [tenants.md](tenants.md) shows.

## Device sync

Device sync is experimental, so confirm with the user before you build it.

1. On the server, open the same database, execute `db.watch(table)` for each synced table, and pass `acceptDeviceSync: true` and an `authenticate` hook to `createServer`. A 0.3.3 server has no such option and accepts device sync from every caller that it admits.
2. On the device, migrate, then execute `db.watch(table)` for each synced table before the app writes to it.
3. Create `new SyncController(db, { url, databaseId, tables, headers, webSocketProtocols })` from `@delali/sirannon-db/client`, and `await sync.start()`. On 0.3.3, `start()` throws `CONNECTION_ERROR` when the server is unreachable, so wrap it in a `try` there and call `start()` again later. Newer releases resolve offline and retry on their own.
4. On a new device, `await sync.downloadSnapshot()` after `start()` resolves, because the live pull carries only the changes made after the device connects.
5. Read `await sync.status()` and confirm that `pendingPushCount` is 0 and `lastError` is `null`.

- A write made before `db.watch` covers its table never reaches the server, and `pendingPushCount` still reads 0.
- Each side applies its own conflict resolver: the device uses `SyncControllerOptions.resolver`, and the server uses its execution target's `applyChanges`.
- Read the rows through `db.live`, which updates when the controller applies a pulled change.

Read `device-sync.md` and `device-sync-recovery.md` in the versioned documentation for snapshots and the migration handshake.
