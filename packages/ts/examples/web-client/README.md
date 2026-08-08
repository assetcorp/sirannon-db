# Sirannon Fulfillment Operations Demo

An inventory console where every list on the page is a live query. The browser opens two live queries over one WebSocket and never fetches a snapshot, never polls, and never applies a change event by hand. Writes go through registered operations, so the data server accepts no SQL from the network at all.

## Setup

This example needs Node.js 22 or newer and pnpm.

The data server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you run anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

The code generator behind `pnpm run codegen` reads that same output. Run the build again whenever you change anything under `packages/ts/src`.

## Run

Start the Sirannon data server and the application server together:

```bash
pnpm --dir packages/ts/examples/web-client run dev
```

Or run them separately:

```bash
pnpm --dir packages/ts/examples/web-client run server
pnpm --dir packages/ts/examples/web-client run app:dev
```

Open `http://localhost:3000`. Set `PORT` to move the application server, and the data server allows that origin automatically.

## What the browser runs

Both panels come from `useLiveQuery`, and each returns rows plus a status:

```tsx
const productsState = useLiveQuery(liveDatabase, main.reads.products, {})
const activityState = useLiveQuery(liveDatabase, main.reads.activity, {})
```

The server re-reads the registered statement when a change lands and sends the row operations that follow from it, so the table updates in place. There is no refresh button on this page because there is nothing for it to do.

Writes use `useCommand`, which returns a stable callback for a registered write:

```tsx
const allocateFromBrowser = useCommand(liveDatabase, main.writes.allocateProduct)
await allocateFromBrowser({ productId: product.id })
```

The mode switcher changes where a write goes. `Write through the app server` calls a TanStack server function that validates the input with Zod and then calls the same registered write over HTTP. `Write from the browser` calls it over the socket the live queries already hold. Reads stay live either way.

## What the server registers

[`src/operations.ts`](src/operations.ts) holds every statement this server will run, keyed by database identifier. A caller sends a name and arguments; the server chooses the SQL. Each write also declares `fromIdentity`, so the server fills the `operator` column from the authenticated caller and a request that supplies `operator` itself fails with `ARGUMENT_NOT_ALLOWED`.

The two demo credentials map to two operators, which is why the change log shows `ops-console` for writes through the app server and `warehouse-floor` for writes from the browser.

`sirannon-codegen` turns that registry into the typed references the client calls it through:

```bash
pnpm --dir packages/ts/examples/web-client run codegen
```

That writes [`src/generated/operations.ts`](src/generated/operations.ts), which is checked in. Regenerate it whenever you change the registry; the generated `registryDigest` is what a live query echoes when it subscribes, and a server serving a different registry refuses with `REGISTRY_MISMATCH`.

## Schema

Two tables, seeded on startup:

- `products` (id, name, price, stock) with five sample records
- `activity` (id, product_name, action, quantity, operator, created_at)

Live queries install their own change tracking, so the server calls no `watch` of its own.

## Environment

```bash
SIRANNON_PORT=9876
HOST=127.0.0.1
PORT=3000
APP_ORIGIN=http://localhost:3000
SIRANNON_ENDPOINT=http://localhost:9876
SIRANNON_DEMO_TOKEN=sirannon-demo-token
VITE_SIRANNON_ENDPOINT=http://localhost:9876
VITE_SIRANNON_DEMO_TOKEN=sirannon-warehouse-token
```

## Security model

This demo is lighter than a production application, and it avoids the unsafe parts people tend to copy from examples.

What this example does:

- Binds the data server to `127.0.0.1` and restricts CORS to the application origin.
- Leaves `acceptSql` at its default, so the five statement routes and their WebSocket messages answer `SQL_NOT_ACCEPTED`. Confirm it with `curl http://localhost:9876/capabilities`, which lists `query.named` and no `query.sql`.
- Requires `Authorization: Bearer <token>` on HTTP routes and a `Sec-WebSocket-Protocol` value derived from a token on the upgrade, and returns an operator identity from `authenticate` rather than a bare pass or fail.
- Validates the WebSocket `Origin` header during the upgrade, which CORS does not cover.
- Checks every argument inside the registered write, so the browser path and the app-server path enforce the same bounds.

What this example does not do:

- It has no real user login, sessions, JWTs, roles, or tenant checks.
- It has no rate limiting, abuse protection, audit logging, or WAF rules.
- It terminates no TLS. Local development uses `http://` and `ws://`.
- The browser token is visible to browser code. Treat it as a local demonstration.

Before adapting this pattern for a public deployment, put the server behind HTTPS and WSS, derive short-lived WebSocket credentials from a real identity layer, keep long-lived secrets out of `VITE_*` variables, and redact authorization and WebSocket protocol values from access logs.
