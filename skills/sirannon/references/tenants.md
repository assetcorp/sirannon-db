# Migrations and a database per tenant

## Migrations

1. On Node.js, keep migrations as files named `<version>_<name>.up.sql` and `<version>_<name>.down.sql`, and load them with `loadMigrations(dir)` from `@delali/sirannon-db/file-migrations`. The loader skips every file with another name.
2. In a browser or Expo bundle, which has no `node:fs`, build them with `migrationsFromFiles` from the core package. Under Vite, pass it `import.meta.glob('./migrations/*.sql', { query: '?raw', import: 'default', eager: true })`.
3. Pass the set to the registry as `new Sirannon({ driver, migrations })`, so that every writable database applies it as it opens. Call `db.migrate(migrations)` only for a database that you open outside that set.
4. Check the result with `await db.appliedMigrations()`.

## A database per tenant, user, or AI agent

Give each tenant its own file, and let the registry open it on first use:

```ts
const sirannon = new Sirannon({
  driver: betterSqlite3(),
  migrations,
  lifecycle: {
    autoOpen: { resolver: createTenantResolver({ basePath: './data/tenants' }) },
    idleTimeout: 300_000,
    maxOpen: 500,
  },
})

const db = await sirannon.resolve(tenantId)
if (!db) throw new Error(`Unknown tenant '${tenantId}'`)
```

- `resolve` returns `undefined` for an identifier outside `[a-zA-Z0-9][a-zA-Z0-9_-]*` or longer than 255 characters, so check its result.
- Create `basePath` before the first `resolve`, because `open` refuses a missing directory.
- `idleTimeout` and `maxOpen` default to off. At a full cap that it cannot free, `resolve` throws `MaxDatabasesError`.
- For AI agents, build the identifier from the boundary that you want to keep, such as `${customerId}-${agentId}`.
- To delete a tenant, `await sirannon.close(id)`, then delete its file with any `-wal` and `-shm` files beside it.
