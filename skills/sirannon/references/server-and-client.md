# The server and the client SDK

## Install the server's peer package

`@delali/sirannon-db/server` serves a registry over HTTP and WebSocket through uWebSockets.js. The npm registry has no package by that name, so install the tagged GitHub release:

```bash
pnpm add -E "uWebSockets.js@github:uNetworking/uWebSockets.js#v20.69.0"
```

When the process cannot load the package, `server.listen()` throws `SERVER_DEPENDENCY_MISSING` with this command in the message. Version 0.3.3 and older fail at the import instead, as SKILL.md describes.

## Serve registered operations

The server refuses SQL from the network by default, and answers a SQL request with status 403 and code `SQL_NOT_ACCEPTED`. Callers reach the data through the reads and writes that you register by name, so the SQL stays on the server:

```ts
import { readBearerToken, RequestDeniedError } from '@delali/sirannon-db'
import { createServer } from '@delali/sirannon-db/server'

interface Identity {
  tenant: string
}

const server = createServer<Identity>(sirannon, {
  port: 9876,
  cors: { origin: ['https://app.example.com'] },
  authenticate: ctx => {
    const identity = verifyToken(readBearerToken(ctx))
    if (!identity) throw new RequestDeniedError(401, 'UNAUTHORIZED', 'Invalid or missing token')
    return identity
  },
  operations: {
    app: {
      reads: {
        listUsers: {
          fromIdentity: { tenant: 'tenant' },
          columns: ['id', 'name'],
          statement: ({ tenant }) => ({ sql: 'SELECT id, name FROM users WHERE tenant = ?', params: [tenant] }),
        },
      },
      writes: {
        addUser: {
          args: ['name'],
          fromIdentity: { tenant: 'tenant' },
          statements: ({ name, tenant }) => ({
            sql: 'INSERT INTO users (name, tenant) VALUES (?, ?)',
            params: [name, tenant],
          }),
        },
      },
    },
  },
})

await server.listen()
```

- The keys under `operations` are database identifiers, and the server looks each one up through `sirannon.resolve`, so tenant databases opened through `lifecycle.autoOpen` work here too.
- `args` lists the arguments that a caller may pass. `fromIdentity` maps an argument to a field of the identity that `authenticate` returns, and the server refuses a caller who passes that argument with `ARGUMENT_NOT_ALLOWED`. Put every tenant or owner boundary in `fromIdentity`.
- A write's `statements` function returns one statement or an array of them, and the server executes an array in one transaction.
- `authenticate` receives the request's lower-case headers, and it runs before every database route and every WebSocket upgrade. Throw `RequestDeniedError` to refuse the request with your own status and code.
- Call `await server.close()` before `await sirannon.shutdown()` on the way out.

## Keep the server safe

- The server binds to `127.0.0.1` on port 9876 by default. Bind to another interface only behind a reverse proxy or a load balancer that terminates TLS, since the server itself serves plain HTTP and WebSocket.
- Set `cors.origin` to the app's own origins, because `cors: true` allows every origin.
- A raw table subscription over WebSocket stays open when the server refuses SQL, so every caller that the server admits can stream each row change of any table. Register an `onBeforeSubscribe` hook on the registry that throws a `RequestDeniedError` for every table and filter that the caller's `identity` may not read. A device's sync subscription passes through the same hook with `deviceId` set, while a live query on a registered read bypasses it:

  ```ts
  const sirannon = new Sirannon({
    driver: betterSqlite3(),
    hooks: {
      onBeforeSubscribe: ({ table, filter, identity }) => {
        if (!mayStream(identity, table, filter)) throw new RequestDeniedError(403, 'FORBIDDEN', `No change stream for '${table}'`)
      },
    },
  })
  ```

- Leave `acceptSql` off unless every caller that `authenticate` admits may execute any statement, such as a service on a private network.
- `acceptDeviceSync` opens routes that write rows into every table, and `acceptBackupRestore` opens a route that replaces a running database. The constructor therefore throws `INVALID_DEVICE_SYNC` or `INVALID_BACKUP_RESTORE` when you set either one without `authenticate`.
- The server caps each HTTP body and WebSocket message at 1 MB, and `maxBodyBytes` changes the cap.

## Call the server from the client SDK

```ts
import { operationRef } from '@delali/sirannon-db'
import { SirannonClient } from '@delali/sirannon-db/client'

const listUsers = operationRef<Record<string, never>, { id: number; name: string }>('listUsers')
const addUser = operationRef<{ name: string }, never>('addUser')

const client = new SirannonClient('http://127.0.0.1:9876', { headers: { Authorization: `Bearer ${token}` } })
const db = client.database('app')

await db.execute(addUser, { name: 'Ada' })
const users = await db.query(listUsers, {})
const live = await db.live(listUsers, {})
```

- The client uses the WebSocket transport by default and reconnects after a dropped connection. Pass `transport: 'http'` for plain requests.
- A browser cannot attach headers to a WebSocket handshake, so a browser client constructed with `headers` alone on the WebSocket transport throws `INVALID_ARGUMENT`. Give it a short-lived ticket in `webSocketProtocols`, built with `toSubprotocolCredential(prefix, ticket)`, and read it in `authenticate` with `readSubprotocolCredential(ctx, prefix)`. A client that also needs `headers` for HTTP requests can pass both options.
- When the server rejects the upgrade with status 401 or 403, the client raises `UNAUTHORIZED` or `FORBIDDEN` and stops reconnecting.

## Generate the operation references

`sirannon-codegen` reads the registry module and writes typed references, so the client never spells an operation name by hand:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --out ./src/generated/operations.ts
```

The registry module exports the registry as `operations` or as its default export, and `--export <name>` picks another export. Keep the registry in a module of its own, because the command imports that module and executes everything at its top level. Since Node.js loads the module directly, a `.ts` registry needs a Node.js release that strips TypeScript types by default, as Node.js 24 does, or a loader such as `tsx` on an older release. The generated file exports one object for each database, so the client calls `db.query(app.reads.listUsers, {})`.

Read `server.md`, `registered-operations.md`, `client-sdk.md`, `code-generation.md`, and `security.md` in the versioned documentation for the routes, the WebSocket protocol, and the other client options.
