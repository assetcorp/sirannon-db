---
name: sirannon
description: Adds Sirannon, the database library that puts real SQLite on every runtime, to an app. Covers choosing the Sirannon package for the app's language, choosing the driver for its runtime and the peer packages, opening databases, migrations, a database for every tenant or AI agent, change subscriptions and live queries, backups, serving databases over HTTP and WebSocket with registered operations, the client SDK, offline device sync, and replication across nodes. Use when the user mentions Sirannon or @delali/sirannon-db, asks to add a database through sirannon.sondelali.com, or works in a project that already depends on @delali/sirannon-db.
license: Apache-2.0
---

# Sirannon

Sirannon puts real SQLite on every runtime that an app touches. A language-neutral specification defines its wire formats, value encodings, and replication rules, which each language's package implements.

## Choose the package for the app's language

| The app's language | Package | Follow |
| --- | --- | --- |
| JavaScript or TypeScript | `@delali/sirannon-db` on npm, the reference implementation | [The @delali/sirannon-db package](#the-delalisirannon-db-package), with the reference files that it links |

The TypeScript package is the only implementation so far. When the app is written in any other language, tell the user that Sirannon has no package for that language yet, and ask how they want to go on before you install anything.

## The @delali/sirannon-db package

These steps and the reference files that they link describe `@delali/sirannon-db`. You install one core package, add the SQLite driver for the runtime, and use the same `Database` API on Node.js, Bun, the browser, and React Native. Every other part, from the server to replication, has an import path of its own, so a bundler leaves out each part that the app never imports.

Follow the steps in order. Each step ends on a check, and the report in the last step lists only the checks that you performed.

### 1. Find the installed version

Read the `version` field of `node_modules/@delali/sirannon-db/package.json`, starting from the folder with the app's own `package.json`. Read the file itself, because the package's `exports` map leaves out `./package.json`, so `require('@delali/sirannon-db/package.json')` throws `ERR_PACKAGE_PATH_NOT_EXPORTED`.

When the package is missing, install it with the project's own package manager at an exact version:

```bash
pnpm add -E @delali/sirannon-db
```

With npm, write `npm install --save-exact`, and with Yarn or Bun, write `yarn add --exact` or `bun add --exact`. On Node.js, the package requires version 22 or newer.

When the installed version is 0.3.3 or older and the user wants the server, gRPC replication, or the etcd coordinator, read [Version 0.3.3 and older](#version-033-and-older) before you write any import.

The step is done when you can name the installed version and its `major.minor` pair, such as `0.3`.

### 2. Treat the installed types as the API

The `.d.ts` files inside the installed package describe exactly the API that the app can call. For each import path that you use, open the package's `package.json`, find that path under `exports`, and read the file that its `types` entry names, following each re-export into the chunk file that it points at. This skill names only the API that its authors confirmed against a built package, but the installed version outranks this file wherever the two differ.

For anything deeper, read the documentation for the installed `major.minor` version:

```text
https://sirannon.sondelali.com/docs/<major.minor>/llms.txt
```

That index lists every page, and every page has a Markdown copy at the URL that the index gives, such as `https://sirannon.sondelali.com/docs/0.3/server.md`. Read the versioned site only. The `docs/` folder in the GitHub repository describes code that npm has not published yet. When the index for the installed version returns 404, the maintainers have not published that version's pages yet, so read the newest index that the site does serve and check every name that you take from it against the installed `.d.ts`.

### 3. Choose the driver for the runtime

Pick the row for the runtime that executes the code. A project with a server and a browser app needs one driver on each side.

| Runtime | Import | Factory | Install | Status |
| --- | --- | --- | --- | --- |
| Node.js | `@delali/sirannon-db/driver/better-sqlite3` | `betterSqlite3()` | `pnpm add -E better-sqlite3` | Stable |
| Node.js 22.13 or newer, with no native build | `@delali/sirannon-db/driver/node` | `nodeSqlite()` | Nothing, since it uses `node:sqlite` | Stable |
| Browser | `@delali/sirannon-db/driver/wa-sqlite` | `waSqlite()` | `pnpm add -E wa-sqlite` | Stable |
| Bun | `@delali/sirannon-db/driver/bun` | `bunSqlite()` | Nothing, since it uses `bun:sqlite` | Experimental |
| React Native with Expo | `@delali/sirannon-db/driver/expo` | `expoSqlite()` | `pnpm add -E expo-sqlite` | Experimental |

Use `better-sqlite3` on Node.js unless the user wants to avoid a native build, in which case use `nodeSqlite()`. Node.js 22.13.0 and 23.4.0 were the first releases to load `node:sqlite` without a flag. Both Node.js drivers support read pools, extensions, the writer worker, and backups.

`better-sqlite3` compiles a native binding in its install script, and pnpm skips a dependency's install script until the project approves it. pnpm 11 fails the install with `ERR_PNPM_IGNORED_BUILDS` and writes a placeholder under `allowBuilds` in `pnpm-workspace.yaml`, so set that entry to `true` and rebuild:

```yaml
allowBuilds:
  better-sqlite3: true
```

```bash
pnpm rebuild better-sqlite3
```

The browser, Bun, and Expo drivers need their own setup, which [references/browser-and-mobile.md](references/browser-and-mobile.md) covers. Tell the user before you build on the Bun or Expo driver, since both are experimental.

The step is done when the driver's import resolves and its peer package loads. For `better-sqlite3`, check the native binding with `node -e "require('better-sqlite3')(':memory:')"`.

### 4. Open a database

On a server, create one `Sirannon` registry per process and open each database through it:

```ts
import { mkdirSync } from 'node:fs'
import { Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'

mkdirSync('./data', { recursive: true })

const sirannon = new Sirannon({ driver: betterSqlite3() })
const db = await sirannon.open('app', './data/app.db')

await db.execute('CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)')
await db.execute('INSERT INTO tasks (title) VALUES (?)', ['Write the release note'])
const open = await db.query<{ id: number; title: string }>('SELECT id, title FROM tasks WHERE done = ?', [0])
```

These behaviours catch agents out:

- Sirannon creates the database file and leaves the directory to you. When the directory is missing, `open` throws `DATABASE_OPEN_FAILED`, so create it first.
- `open` throws `DATABASE_ALREADY_EXISTS` for an identifier that the registry already holds, so call `sirannon.get(id)` first in code that can reach the same database twice.
- Pass every value through a `?` placeholder or a named parameter, since the driver binds parameters separately from the SQL text.
- `db.transaction(async tx => { ... })` commits when the callback resolves and rolls back when it throws. Call `tx.execute` and `tx.query` inside it, because only the calls on `tx` belong to the transaction.
- Tables whose names start with `_sirannon` belong to Sirannon, and the query API refuses them with `FORBIDDEN_SQL`.
- An integer beyond the safe range returns as a `bigint`, and so does `ChangeEvent.seq`, so `JSON.stringify` throws on those values until you convert them.
- On shutdown, `await sirannon.shutdown()` closes every database, and `sirannon.close(id)` closes one.

Sirannon opens each database in WAL mode with `synchronous=NORMAL` and, on a driver that supports several connections, a pool of four read connections. `DatabaseOptions`, the third argument of `open`, changes each of those defaults.

The step is done when a write followed by a read returns the row that you wrote.

### 5. Build what the user asked for

Read the reference file for each capability that the user names, and read only those files.

| The user asks for | Read | Status |
| --- | --- | --- |
| Schema migrations, a database for every tenant, user, or AI agent, or idle closing | [references/migrations-and-tenants.md](references/migrations-and-tenants.md) | Stable |
| Reacting to row changes, live query results, React hooks, query hooks, or metrics | [references/changes-and-live-queries.md](references/changes-and-live-queries.md) | Stable |
| Backups, scheduled copies, or restoring to a moment | [references/backups.md](references/backups.md) | Stable |
| An API over HTTP or WebSocket, a browser or Node.js client, or typed operation references | [references/server-and-client.md](references/server-and-client.md) | Stable |
| A database in the browser or on a phone, or offline sync between a device and a server | [references/browser-and-mobile.md](references/browser-and-mobile.md) | Device sync, Bun, and Expo are experimental |
| Read replicas, several nodes, or automatic failover | [references/replication.md](references/replication.md) | Replication is stable, and etcd failover is experimental |

Before you build on device sync, the Bun or Expo driver, or etcd failover, tell the user that the package marks it as experimental and confirm that they still want it.

The step is done when the code that the user asked for type-checks against the installed `.d.ts` and executes once without an error.

### 6. Report

Tell the user what you added, and list each check that you performed with its result: the version that you found, the driver and peer packages that you installed, the type check, and each script or request that you executed. Name every experimental part that the app now depends on, and every step that you could not check, such as a browser that you could not open. Leave out any claim that you did not check.

### Version 0.3.3 and older

In 0.3.3 and every earlier release, the server, gRPC, and etcd entry points import their optional packages when the module loads. A missing package therefore fails the import itself with Node's `ERR_MODULE_NOT_FOUND`, before any Sirannon code reports a named error. Newer releases load those packages later and throw `SERVER_DEPENDENCY_MISSING`, `TRANSPORT_DEPENDENCY_MISSING`, or `COORDINATOR_DEPENDENCY_MISSING` with the install command in the message.

Upgrade to the newest release when the project allows it:

```bash
pnpm add -E @delali/sirannon-db@latest
```

When the project has to stay on 0.3.3, install each optional package before the first import that needs it:

| Import | Packages |
| --- | --- |
| `@delali/sirannon-db/server` | `pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"` |
| `@delali/sirannon-db/transport/grpc` | `pnpm add -E @grpc/grpc-js @bufbuild/protobuf grpc-health-check` |
| `@delali/sirannon-db/replication/coordinator/etcd` | `pnpm add -E etcd3` |

A 0.3.3 server also serves the device sync routes to every caller that it admits, because 0.3.3 has no `acceptDeviceSync` option, so give every 0.3.3 server an `authenticate` hook.
