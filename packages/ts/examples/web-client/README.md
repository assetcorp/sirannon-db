# Sirannon Fulfillment Operations Demo

A fulfillment operations app that demonstrates Sirannon as a networked SQLite data layer with HTTP operations, WebSocket subscriptions, CDC updates, and transactions working together.

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

Both modes keep WebSocket subscriptions in the browser for live CDC events. The UI loads one initial snapshot, then applies product and activity changes from the stream instead of refetching after every mutation.

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

## Production notes

This is still an example. For an internet-facing application, keep arbitrary SQL away from public browsers, enforce real user authentication on domain endpoints, and authenticate WebSocket upgrades through an auth layer that the deployment platform supports.
