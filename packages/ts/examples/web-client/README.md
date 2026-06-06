# Sirannon Fulfillment Operations Demo

This example demonstrates Sirannon as a networked SQLite data layer with HTTP operations, WebSocket subscriptions, CDC updates, and transactions.

## Run

Start the Sirannon data server and application server together:

```bash
cd packages/ts/examples/web-client
pnpm run dev
```

Or run them separately:

```bash
pnpm run server   # Sirannon data server on port 9876
pnpm run app:dev  # application server on port 3000
```

Open `http://localhost:3000`.

## Architecture

The demo has two UI modes:

- **Application API**: the browser calls server-side domain actions. Those actions validate inputs and call the Sirannon HTTP API from the server.
- **Direct Data API**: the browser calls the Sirannon HTTP API directly. The data server limits this mode with loopback binding, restricted CORS, bearer auth for HTTP, and a demo SQL allowlist.

Both modes keep WebSocket subscriptions in the browser for live CDC events. The UI loads one initial snapshot, then applies product and activity changes from the stream instead of refetching after every mutation. WebSocket upgrades are limited to configured browser origins and must include the configured demo auth subprotocol.

## Environment

Optional configuration:

```bash
PORT=9876
HOST=127.0.0.1
APP_ORIGIN=http://localhost:3000
SIRANNON_ENDPOINT=http://localhost:9876
SIRANNON_DEMO_TOKEN=sirannon-demo-token
VITE_SIRANNON_ENDPOINT=http://localhost:9876
VITE_SIRANNON_DEMO_TOKEN=sirannon-demo-token
```

## Features

- Inventory ledger with Allocate and Receive actions
- Create Item form for adding catalog records
- Live activity feed updated through CDC
- Reset Database action that clears rows and resets SQLite autoincrement IDs
- Mode switcher for server-function actions versus direct driver access
- Manual refresh for explicit snapshot resync

## Server schema

Two tables seeded on startup:

- `products` (id, name, price, stock) with 5 sample products
- `activity` (id, product_name, action, quantity, created_at) tracking all changes

Both tables have CDC watches enabled for real-time change detection.

## Prerequisites

- Node.js >= 22
- pnpm

From the monorepo root:

```bash
pnpm install
pnpm --filter @delali/sirannon-db build
```

## Security model

This demo is intentionally lighter than a production application, but it avoids the unsafe parts people tend to copy from examples.

What this example does:

- Binds the Sirannon data server to `127.0.0.1` by default.
- Restricts browser CORS to `APP_ORIGIN`.
- Requires `Authorization: Bearer <SIRANNON_DEMO_TOKEN>` for HTTP database routes.
- Authenticates browser WebSocket upgrades with a `Sec-WebSocket-Protocol` value derived from `SIRANNON_DEMO_TOKEN`.
- Validates the WebSocket `Origin` header against `APP_ORIGIN`.
- Keeps single-statement HTTP writes disabled and only allows the SQL statements this demo needs.
- Uses server-side domain actions for the default mode, so the browser does not need to construct SQL for normal application workflows.

What this example does not do:

- It does not implement real user login, sessions, JWTs, roles, tenant checks, or permission checks.
- It does not include rate limiting, abuse protection, audit logging, or WAF rules.
- It does not terminate TLS itself. Local development uses `http://` and `ws://`.
- The demo token is visible to browser code in Direct Data API mode. Treat that mode as a local demonstration of the client SDK, not as a production browser security boundary.

Before adapting this pattern for a public deployment:

- Put the Sirannon server behind HTTPS/WSS through a reverse proxy, load balancer, or platform edge.
- Keep arbitrary SQL out of browser clients. Prefer server-side application actions or a narrow `resolveExecutionTarget` allowlist.
- Use a real identity layer at the application boundary and derive short-lived WebSocket credentials from that identity.
- Validate WebSocket `Origin` during the upgrade. CORS does not protect WebSocket handshakes.
- Do not put long-lived secrets in `VITE_*` environment variables or other browser-visible configuration.
- Redact authorization values and WebSocket auth protocol values from access logs.
