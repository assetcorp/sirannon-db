# Sirannon Fulfillment Operations Demo

This example is an inventory console in which every list on the page is a live query. The browser opens two live queries over one WebSocket, and it never fetches a snapshot, polls, or applies a change event by hand. Writes go through registered operations, so the data server accepts no SQL from the network.

## Setup

This example needs Node.js 22 or newer and pnpm.

The data server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

The code generator behind `pnpm run codegen` imports that same output. Build the package again whenever you change anything under `packages/ts/src`.

## Run

Start the Sirannon data server and the application server together:

```bash
pnpm --dir packages/ts/examples/web-client run dev
```

Or start them separately:

```bash
pnpm --dir packages/ts/examples/web-client run server
pnpm --dir packages/ts/examples/web-client run app:dev
```

Open `http://localhost:3000`. Set `PORT` to move the application server to another port, and the data server then allows that origin automatically.

## What the browser runs

Both panels come from `useLiveQuery`, which returns the rows and a status:

```tsx
const productsState = useLiveQuery(liveDatabase, main.reads.products, {})
const activityState = useLiveQuery(liveDatabase, main.reads.activity, {})
```

When a write changes a table, the server applies that change to the rows that each live query holds and sends the resulting row operations, so the table on the page updates in place. The page has no refresh button, because the live queries keep every row current.

Writes use `useCommand`, which returns a stable callback for a registered write:

```tsx
const allocateFromBrowser = useCommand(liveDatabase, main.writes.allocateProduct)
await allocateFromBrowser({ productId: product.id })
```

The mode switcher changes the path that a write takes. `Write through the app server` calls a TanStack server function that validates the input with Zod and then calls the same registered write over HTTP. `Write from the browser` calls it over the socket that the live queries already use. Reads stay live in both modes.

## What the server registers

[`src/operations.ts`](src/operations.ts) holds every statement that this server executes, keyed by database identifier. A caller sends a name and arguments, and the server chooses the SQL. Every write except `resetInventory` also declares `fromIdentity`, so the server fills the `operator` column from the authenticated caller, and a request that supplies `operator` itself fails with `ARGUMENT_NOT_ALLOWED`.

The two demo credentials map to two operators, which is why the change log shows `ops-console` for writes through the app server and `warehouse-floor` for writes from the browser.

`sirannon-codegen` turns that registry into the typed references that the client uses to call it:

```bash
pnpm --dir packages/ts/examples/web-client run codegen
```

That command writes [`src/generated/operations.ts`](src/generated/operations.ts), which the repository tracks. Regenerate it whenever you change the registry, because a live query sends the generated `registryDigest` when it subscribes, and a server with a different registry rejects the subscription with `REGISTRY_MISMATCH`.

## Schema

The data server creates two tables and seeds the first one on startup:

- `products` (id, name, price, stock) starts with five sample records.
- `activity` (id, product_name, action, quantity, operator, created_at) records each allocation, each receipt of stock, and each new product.

Each live query installs its own change tracking, so the server makes no `watch` call of its own.

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

This demo has fewer protections than a production application, although it leaves out the unsafe patterns that people tend to copy from examples.

What this example does:

- It binds the data server to `127.0.0.1` and restricts CORS to the application origin.
- It leaves `acceptSql` at its default, so the five statement routes and their WebSocket messages fail with `SQL_NOT_ACCEPTED`. Confirm it with `curl http://localhost:9876/capabilities`, which lists `query.named` and no `query.sql`.
- It requires `Authorization: Bearer <token>` on HTTP routes and a `Sec-WebSocket-Protocol` value derived from a token on the upgrade, and `authenticate` returns an operator identity, not a bare pass or fail.
- It checks the WebSocket `Origin` header during the upgrade, which CORS does not cover.
- It checks every argument inside the registered write, so the browser path and the app-server path enforce the same bounds.

What this example leaves out:

- It has no real user login, sessions, JWTs, roles, or tenant checks.
- It has no rate limiting, abuse protection, audit logging, or WAF rules.
- It terminates no TLS, so local development uses `http://` and `ws://`.
- Browser code can read the browser token, so treat it as a local demonstration.

Before you adapt this pattern for a public deployment, put the server behind HTTPS and WSS, derive short-lived WebSocket credentials from a real identity layer, keep long-lived secrets out of `VITE_*` variables, and redact authorization and WebSocket protocol values from access logs.
