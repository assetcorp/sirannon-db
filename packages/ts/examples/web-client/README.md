# Sirannon DB - Product Inventory Demo

A live product inventory that demonstrates real-time reactivity using CDC, HTTP mutations, WebSocket subscriptions, and transactions working together.

## Run

Start both server and client:

```bash
cd packages/ts/examples/web-client
pnpm start
```

Or run them separately:

```bash
pnpm run server   # starts Sirannon server on port 9876
pnpm dev          # starts Vite dev server on port 5174
```

Open the Vite URL in your browser to see the product table and live activity feed.

## Architecture

The demo uses two `SirannonClient` instances, each handling a specific transport:

- **HTTP client** handles mutations (sell, restock, add product) and initial data fetches. Transactions ensure stock changes and activity logs are written atomically.
- **WebSocket client** powers CDC subscriptions that push live updates to the UI whenever products or activity records change.

This two-client pattern mirrors how a production app would wire up Sirannon DB: HTTP for request/response operations, WebSocket for real-time streaming.

## Features

- **Product table** with inline Sell and Restock buttons
- **Add Product form** for creating new inventory items
- **Live activity feed** updated through CDC (no polling)
- **Atomic transactions** that pair stock changes with activity logs
- **Connection status** indicator in the header
- **CDC-driven UI** where product rows update in place without full reloads

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
