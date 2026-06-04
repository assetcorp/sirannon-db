# Sirannon Distributed Entitlements

A production-style example that runs Sirannon as a three-node, coordinator-backed entitlement control plane.

The app models a SaaS entitlement service: billing events update customer plans, usage events decrement quota with idempotency keys, every mutation is written through Sirannon, and the React dashboard observes the replicated SQLite layer with live subscriptions.

## What It Runs

- Three durable Sirannon data nodes: `node-a`, `node-b`, `node-c`
- etcd coordinator authority with Sirannon-owned failover
- gRPC replication over local mTLS certificates
- Toxiproxy links for coordinator and replication failure controls
- TanStack Start dashboard on `http://127.0.0.1:3001`

## Start

From this directory:

```sh
pnpm run dev
```

The `dev` script starts Docker Compose for the cluster and Vite for the TanStack Start app.

Useful scripts:

```sh
pnpm run cluster:up
pnpm run cluster:down
pnpm run cluster:reset
pnpm run app:dev
pnpm run typecheck
pnpm run build
```

## Dashboard Workflows

- Create a customer entitlement record.
- Record usage with an idempotency key.
- Replay duplicate usage and verify it does not double-decrement quota.
- Apply billing webhook events with monotonic entitlement versions.
- Isolate the current primary from etcd through Toxiproxy.
- Heal proxy links and watch the cluster converge.

## Network Topology

The browser and app server use public localhost HTTP endpoints:

- `http://127.0.0.1:7301/db/entitlements`
- `http://127.0.0.1:7302/db/entitlements`
- `http://127.0.0.1:7303/db/entitlements`

The data nodes use Docker-internal links:

- etcd through `toxiproxy:4101`, `toxiproxy:4102`, `toxiproxy:4103`
- gRPC replication through `toxiproxy:5101`, `toxiproxy:5102`, `toxiproxy:5103`

Coordinator discovery advertises localhost endpoints so the browser can route after failover.

## Security Boundary

This is a local example, not a deployable auth template. The cluster requires a bearer token for HTTP and a WebSocket subprotocol token for subscriptions. The default token is intentionally local:

```sh
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

Mutations go through TanStack server functions with zod validation and static SQL statements. The browser-visible WebSocket token is used only to demonstrate live local subscriptions.

## Environment

Optional app variables:

```sh
SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
TOXIPROXY_URL=http://127.0.0.1:8474
VITE_SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```
