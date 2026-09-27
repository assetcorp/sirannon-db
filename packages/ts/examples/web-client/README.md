# Sirannon Fulfillment Operations Demo

This example is an inventory console in which every list on the page is a live query. The browser opens two live queries over one WebSocket, through which the server sends every change to their rows. The data server executes no SQL from the network, so the app sends every write as a call to a registered operation.

## Setup

To start this example, you need Node.js 22 or newer and pnpm.

The data server and the browser app both import `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

The code generator behind `pnpm run codegen` imports that same output. Build the package again whenever you change anything under `packages/ts/src`.

## Start the example

Start the Sirannon data server and the application server together:

```bash
pnpm --dir packages/ts/examples/web-client run dev
```

Or start them separately:

```bash
pnpm --dir packages/ts/examples/web-client run server
pnpm --dir packages/ts/examples/web-client run app:dev
```

Open `http://localhost:3000`. When you set `PORT` to move the application server to another port, the data server sets its CORS origin to `http://localhost:<PORT>`, unless you also set `APP_ORIGIN`.

## The browser code

The app fills both panels from `useLiveQuery`, which returns the rows and a status:

```tsx
const productsState = useLiveQuery(liveDatabase, main.reads.products, {})
const activityState = useLiveQuery(liveDatabase, main.reads.activity, {})
```

When a write changes a table, the server applies that change to the rows of each live query and sends the resulting row operations to the browser. The app then updates each table in place, so the page has no refresh button.

The app sends writes through `useCommand`, which returns a stable callback for a registered write:

```tsx
const allocateFromBrowser = useCommand(liveDatabase, main.writes.allocateProduct)
await allocateFromBrowser({ productId: product.id })
```

The mode switcher sets how the app sends each write. In `Write through the app server` mode, the app calls a TanStack server function, which validates the input with Zod and then calls the registered write over HTTP. In `Write from the browser` mode, the app calls the same registered write over the same WebSocket as the live queries. Reads stay live in both modes.

## The server registry

[`src/operations.ts`](src/operations.ts) contains every statement that this server executes, keyed by database identifier. A caller sends a name and arguments, from which the server builds the SQL. Every write except `resetInventory` also declares `fromIdentity`, so the server fills the `operator` column from the authenticated caller. When a request includes `operator` itself, the server responds with `ARGUMENT_NOT_ALLOWED`.

The two demo credentials map to two operators, which is why the change log shows `ops-console` for writes through the app server and `warehouse-floor` for writes from the browser.

From that registry, `sirannon-codegen` generates the typed references for the client:

```bash
pnpm --dir packages/ts/examples/web-client run codegen
```

That command writes [`src/generated/operations.ts`](src/generated/operations.ts), which git tracks. Regenerate it whenever you change the registry, because the client sends the generated `registryDigest` with every live query subscription. When that digest differs from the server's registry, the server responds to the subscription with a `REGISTRY_MISMATCH` error.

## Schema

The data server creates two tables and seeds the first one on startup:

- `products` (id, name, price, stock) contains five sample records at startup.
- The registered writes add a row to `activity` (id, product_name, action, quantity, operator, created_at) for each allocation, each receipt of stock, and each new product.

When the client subscribes to a live query, the server calls `watch` on the table that the query selects from, so the data server code has no `watch` call of its own.

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

This demo has fewer protections than a production application.

Protections in this example:

- The data server binds to `127.0.0.1`, and its CORS list contains only the application origin.
- `acceptSql` stays at its default, so the server responds to the five statement routes and their WebSocket messages with `SQL_NOT_ACCEPTED`. Confirm it with `curl http://localhost:9876/capabilities`, whose response lists `query.named` and omits `query.sql`.
- Every HTTP request must include `Authorization: Bearer <token>`, and every WebSocket upgrade must include a `Sec-WebSocket-Protocol` value derived from a token.
- The `authenticate` hook returns an operator identity for each caller, and the registered writes store it in the `operator` column.
- The server checks the WebSocket `Origin` header during the upgrade, which CORS does not cover.
- Each registered write checks its arguments, so the same bounds apply to writes from the browser and to writes through the app server.

Protections that this example lacks:

- The example has no user login, sessions, JWTs, roles, or tenant checks.
- The example has no rate limiting, abuse protection, audit logging, or WAF rules.
- The example terminates no TLS, so you connect over `http://` and `ws://` locally.
- Browser code can read the browser token, so treat the example as a local demonstration only.

Before you adapt this pattern for a public deployment, put the server behind HTTPS and WSS, derive short-lived WebSocket credentials from a real identity layer, keep long-lived secrets out of `VITE_*` variables, and redact authorization and WebSocket protocol values from access logs.
