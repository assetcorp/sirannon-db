---
name: sirannon
description: Adds a Sirannon SQLite database to an app, either embedded in the app's own process or served over HTTP and WebSocket to an app in any language. Covers drivers and peer packages, migrations, a database per tenant or AI agent, change subscriptions and live queries, backups, registered operations, the client SDK, offline device sync, and replication. Use when the user mentions Sirannon or @delali/sirannon-db, asks to add a database through sirannon.sondelali.com, or works in a project that already depends on @delali/sirannon-db.
license: Apache-2.0
---

# Sirannon

Copy this checklist and tick each item as you finish it:

```text
- [ ] 1. Choose embedded or served
- [ ] 2. Find the installed version and its API
- [ ] 3. Install the driver
- [ ] 4. Build what the user asked for
- [ ] 5. Verify and report
```

## 1. Choose embedded or served

Embed Sirannon when the app is JavaScript or TypeScript and one process owns the data. `@delali/sirannon-db` is the only embeddable package.

Serve it in every other case: an app in another language, several services sharing one database, or browsers and devices reaching the data over the network. The server is a small Node.js service built on `@delali/sirannon-db/server`, and apps call it over HTTP or WebSocket. Read [references/server.md](references/server.md) before you write it.

## 2. Find the installed version and its API

1. Read `version` from `node_modules/@delali/sirannon-db/package.json`. `require('@delali/sirannon-db/package.json')` fails, because the exports map leaves that file out.
2. When the package is missing, install it at an exact version with the project's package manager, such as `pnpm add -E @delali/sirannon-db`. It requires Node.js 22 or newer.
3. Take every name and option from the installed `.d.ts` files, which the `types` entries of the package's `exports` map point to.
4. For anything deeper, read `https://sirannon.sondelali.com/docs/<major.minor>/llms.txt` and its `.md` pages. Leave the repository's `docs/` folder alone, because it describes unpublished code.

## 3. Install the driver

| Runtime | Import and factory | Install |
| --- | --- | --- |
| Node.js | `driver/better-sqlite3`, `betterSqlite3()` | `better-sqlite3` |
| Node.js 22.13+, no native build | `driver/node`, `nodeSqlite()` | Nothing |
| Browser | `driver/wa-sqlite`, `waSqlite()` | `wa-sqlite` |
| Bun (experimental) | `driver/bun`, `bunSqlite()` | Nothing |
| React Native with Expo (experimental) | `driver/expo`, `expoSqlite()` | `expo-sqlite` |

Default to `better-sqlite3` on Node.js. Every import path starts with `@delali/sirannon-db/`.

## 4. Build what the user asked for

Open one registry per process with `new Sirannon({ driver })`, and open each database with `await sirannon.open(id, path)`. The same two calls open a database in the browser.

Read the reference file for each capability that the task needs:

| When the task needs | Read |
| --- | --- |
| A server, a client SDK, or access from another language | [references/server.md](references/server.md) |
| Migrations, or a database per tenant, user, or AI agent | [references/tenants.md](references/tenants.md) |
| Change subscriptions, live queries, React hooks, or query hooks | [references/changes.md](references/changes.md) |
| Backups or a restore to a moment | [references/backups.md](references/backups.md) |
| A browser or phone database, or offline device sync | [references/devices.md](references/devices.md) |
| Read replicas or failover across nodes | [references/replication.md](references/replication.md) |

Device sync, etcd failover, and the Bun and Expo drivers are experimental. Tell the user so before you build on one, and wait for them to confirm.

## 5. Verify and report

1. Type-check the code against the installed package.
2. Execute it once, and confirm that a write followed by a read returns the row.
3. Fix whatever fails and repeat both checks until they pass.
4. Report what you added, each check with its result, every experimental part in use, and every check that you could not perform.

## Gotchas

- `open` throws `DATABASE_OPEN_FAILED` when the directory is missing, so create it first. It throws `DATABASE_ALREADY_EXISTS` for an open identifier, so call `sirannon.get(id)` first.
- pnpm 11 fails the `better-sqlite3` install with `ERR_PNPM_IGNORED_BUILDS`. Set `better-sqlite3: true` under `allowBuilds` in `pnpm-workspace.yaml`, then execute `pnpm rebuild better-sqlite3`.
- `db.on(table).subscribe` receives nothing until `await db.watch(table)` has executed, and it reports no error either. A watch misses every write made before it.
- An `async` `onBeforeQuery` hook makes every query throw. Write it synchronously, and throw `HookDeniedError` to refuse a statement.
- Integers beyond the safe range, and `ChangeEvent.seq`, arrive as `bigint`, which `JSON.stringify` refuses.
- A live query or a subscription keeps no Node.js process alive, so a script that awaits only an update exits first. Give that wait a `setTimeout` deadline.
- Tables named `_sirannon*` belong to Sirannon, and the query API refuses them with `FORBIDDEN_SQL`.
- Version 0.3.3 and older fail at import, with `ERR_MODULE_NOT_FOUND`, when the server, gRPC, or etcd entry point lacks its peer package. Upgrade with `pnpm add -E @delali/sirannon-db@latest`, or install the peers that [references/server.md](references/server.md) and [references/replication.md](references/replication.md) list before the first import.
