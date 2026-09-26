# Serving Sirannon

## 1. Install the server's peer package

```bash
pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"
```

npm has no package by that name, so install it from GitHub exactly as written. A missing package makes `server.listen()` throw `SERVER_DEPENDENCY_MISSING`, and on 0.3.3 the import itself fails.

## 2. Write the server

```ts
import { mkdirSync } from 'node:fs'
import { createTenantResolver, readBearerToken, RequestDeniedError, Sirannon } from '@delali/sirannon-db'
import { betterSqlite3 } from '@delali/sirannon-db/driver/better-sqlite3'
import { createServer } from '@delali/sirannon-db/server'

mkdirSync('./data/tenants', { recursive: true })

const sirannon = new Sirannon({
  driver: betterSqlite3(),
  migrations,
  lifecycle: { autoOpen: { resolver: createTenantResolver({ basePath: './data/tenants' }) } },
})

const server = createServer<{ tenant: string }>(sirannon, {
  cors: { origin: ['https://app.example.com'] },
  authenticate: ctx => {
    const identity = verifyToken(readBearerToken(ctx))
    if (!identity || identity.tenant !== ctx.databaseId) throw new RequestDeniedError(403, 'FORBIDDEN', 'Not your database')
    return identity
  },
  operations,
})

await server.listen()
```

`operations` maps each database identifier to its `reads` and `writes`, and the server matches that identifier exactly. With a database per tenant, build the object with one entry per tenant identifier that the server may serve. Take the shape of each entry from `ReadOperation` and `WriteOperation` in the installed types, and read `registered-operations.md` in the versioned documentation.

## 3. Keep it safe

- Refuse every caller in `authenticate` that may not use `ctx.databaseId`. Each tenant has a database of its own, so this check is the tenant boundary, and it covers every HTTP route and every WebSocket upgrade.
- Fill every owner or tenant argument through `fromIdentity`, so that a caller who sends it receives `ARGUMENT_NOT_ALLOWED`.
- Leave `acceptSql` off, which is the default, unless every caller that `authenticate` admits may execute any statement. Give callers live results through `db.live` on a registered read.
- To stream a table's changes with `acceptSql` off, register an `onBeforeSubscribe` hook that refuses each table the caller may not read in full. A 0.3.3 server streams every table without the hook, so register it there too.
- List the app's origins in `cors.origin`, since `cors: true` admits every origin.
- Keep the default bind of `127.0.0.1` on port 9876, and put a TLS-terminating proxy in front before anything outside the machine connects.
- `acceptDeviceSync` and `acceptBackupRestore` each need an `authenticate` hook, and the constructor throws without one.

## 4. Call it

From JavaScript or TypeScript, use `SirannonClient` from `@delali/sirannon-db/client` with references that `sirannon-codegen` generates:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --out ./src/generated/operations.ts
```

Keep the registry in a module with no side effects, because the command imports it. A `.ts` registry needs Node.js 24, or a loader such as `tsx` on older releases.

In a browser, pass a short-lived ticket in `webSocketProtocols`, built with `toSubprotocolCredential(prefix, ticket)`, and read it in `authenticate` with `readSubprotocolCredential(ctx, prefix)`. A browser client with `headers` alone throws `INVALID_ARGUMENT`, because a browser sends no header on a WebSocket handshake.

From any other language, call the routes directly with the caller's credentials:

```http
POST /db/acme/query/listNotes
Authorization: Bearer <token>
Content-Type: application/json

{"args": {}}
```

A read answers `{"rows": [...]}`, and `POST /db/{id}/execute/{name}` answers `{"results": [{"changes": 1, "lastInsertRowId": 1}]}`. Read `value-encoding.md` in the versioned documentation before you send or read 64-bit integers or BLOBs, and `server.md` there for the WebSocket messages.
