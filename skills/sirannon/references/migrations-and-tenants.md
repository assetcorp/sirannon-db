# Migrations and a database for every tenant

## Migrations

A migration is an object with a `version`, a `name`, an `up` statement, and an optional `down` statement. `up` and `down` each take a SQL string or a function that receives a `Transaction`. `db.migrate(migrations)` applies each version that the database lacks, inside a transaction, and records a checksum of its content. A second call with the same set applies nothing and reports each version under `skipped`.

```ts
const migrations = [
  {
    version: 1,
    name: 'create_tasks',
    up: 'CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)',
    down: 'DROP TABLE tasks',
  },
]

const result = await db.migrate(migrations)
```

`result.applied` lists the versions that the call applied, and `await db.appliedMigrations()` lists every version on record with its checksum. `db.rollback(migrations, version)` undoes every applied version above `version` through its `down` statement, and without `version` it undoes the newest one alone. Sirannon applies each version once, even when two processes migrate one database at the same moment.

Keep migrations as files when the project holds SQL on disk. Name each file `<version>_<name>.up.sql` or `<version>_<name>.down.sql`, as in `001_create_tasks.up.sql`, since the loader skips every file with another name. On Node.js, load the folder with the file loader:

```ts
import { loadMigrations } from '@delali/sirannon-db/file-migrations'

const migrations = loadMigrations('./migrations')
```

`@delali/sirannon-db/file-migrations` reads the disk through `node:fs`, so a browser or Expo bundle can't import it. There, build the set from SQL that the bundler inlines, with `migrationsFromFiles` from the core package. Under Vite, the call looks like this:

```ts
import { migrationsFromFiles } from '@delali/sirannon-db'

const migrations = migrationsFromFiles(
  import.meta.glob('./migrations/*.sql', { query: '?raw', import: 'default', eager: true }),
)
```

`migrationsFromFiles` reads the file name from the end of each key and throws `MIGRATION_VALIDATION_ERROR` for a key whose value is anything other than SQL text.

## One set of migrations for every database

Pass the set to the registry, and Sirannon applies it to every writable database as that database opens:

```ts
const sirannon = new Sirannon({ driver: betterSqlite3(), migrations })
```

`migrations` also takes a function that returns the set, or a promise of it, for a set that the app loads lazily.

## A database for every tenant, user, or AI agent

Give each tenant a database file of its own, and let the registry open each file the first time that a caller asks for it:

```ts
import { mkdirSync } from 'node:fs'
import { createTenantResolver, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

mkdirSync('./data/tenants', { recursive: true })

const sirannon = new Sirannon({
  driver: betterSqlite3(),
  migrations,
  lifecycle: {
    autoOpen: { resolver: createTenantResolver({ basePath: './data/tenants' }) },
    idleTimeout: 300_000,
    maxOpen: 500,
  },
})

const acme = await sirannon.resolve('acme')
```

- `sirannon.resolve(id)` returns the open database, opens `./data/tenants/<id>.db` through the resolver when none is open, and migrates it on the way.
- The resolver accepts an identifier of 1 to 255 characters that starts with a letter or a digit and continues with letters, digits, underscores, or hyphens. For every other identifier, `resolve` returns `undefined`, so check for it before you use the result. `sanitizeTenantId(id)` applies the same test.
- `idleTimeout` closes a database after that many idle milliseconds, and `maxOpen` closes the least recently used database when one more would pass the cap. Both default to off. When the cap stays full, `resolve` throws `MaxDatabasesError`.
- By default, the server looks up every database that a request names through `sirannon.resolve`, so the same registry serves tenants over the network with no extra code.
- A database for each AI agent follows the same pattern. Build the identifier from the boundary that you want to keep, such as `${customerId}-${agentId}`, so that one agent's queries can only reach its own file.

To delete a tenant, call `await sirannon.close(id)` and then delete the database file with any `-wal` and `-shm` files beside it.

Read `migrations.md`, `hooks-metrics-and-lifecycle.md`, and `agent-databases.md` in the versioned documentation for baselines, squashing old history, and the agent patterns.
