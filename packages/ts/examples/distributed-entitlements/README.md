# Run a three-node Sirannon entitlement cluster

This example runs Sirannon as a three-node, coordinator-backed entitlement control plane. Each node has its own durable SQLite database. Sirannon replicates changes between those databases and uses etcd to manage primary authority and automatic failover.

The application models a SaaS entitlement service. Billing events update customer plans, usage events decrement quota through idempotent transactions, and the dashboard shows cluster state and replicated data as they change.

## What runs

- Three Sirannon data nodes run as `node-a`, `node-b`, and `node-c`.
- One etcd service handles authority, leases, primary terms, and the in-sync set.
- gRPC replication connects the Sirannon nodes and is protected by locally generated mTLS certificates.
- Toxiproxy links provide coordinator and replication failure controls.
- A TanStack Start dashboard runs at `http://127.0.0.1:3001`.

## How traffic moves

The example uses three separate network paths:

| Path | Protocol | Purpose |
| --- | --- | --- |
| TanStack server functions to Sirannon nodes | HTTP | Queries, transactions, and cluster status |
| Browser to Sirannon nodes | WebSocket | Live CDC subscriptions for dashboard refreshes |
| Sirannon node to Sirannon node | gRPC with mTLS | Replication batches, acknowledgements, write forwarding, and first sync |
| Sirannon nodes to etcd | etcd gRPC through Toxiproxy | Primary authority, leases, node sessions, and replication-group metadata |

WebSocket is an application client transport in this example. It is not used for node-to-node replication. `cluster-node.ts` creates a `GrpcReplicationTransport` for replication, while `direct-client.ts` creates a WebSocket client for browser subscriptions.

Application traffic uses these localhost endpoints:

- `http://127.0.0.1:7301/db/entitlements`
- `http://127.0.0.1:7302/db/entitlements`
- `http://127.0.0.1:7303/db/entitlements`

The containers use Docker-internal Toxiproxy links:

- etcd through `toxiproxy:4101`, `toxiproxy:4102`, and `toxiproxy:4103`
- gRPC replication through `toxiproxy:5101`, `toxiproxy:5102`, and `toxiproxy:5103`

Each node advertises its localhost HTTP endpoint through coordinator discovery. The HTTP and WebSocket clients use those records to find the current primary and eligible read replicas after failover.

## Start the example

From this directory, run:

```sh
pnpm run dev
```

The `dev` script starts the Docker Compose cluster and the TanStack Start application. The first build creates the local certificate authority and one certificate for each Sirannon node.

Useful scripts:

```sh
pnpm run cluster:up
pnpm run cluster:down
pnpm run cluster:reset
pnpm run app:dev
pnpm run typecheck
pnpm run build
```

`cluster:down` and `cluster:reset` remove the example's Docker volumes, including all three SQLite databases and etcd state.

## Dashboard workflows

- Create a customer and its entitlement record in one transaction.
- Record usage with an idempotency key.
- Replay the same usage event and verify the quota changes once.
- Apply billing events with monotonic entitlement versions.
- Isolate the current primary from etcd through Toxiproxy and observe failover.
- Restore the coordinator and replication links and watch eligible nodes converge.

The mutation path checks majority write availability before sending a write. The coordinator still determines whether the current primary has authority, and the replication engine uses majority write concern by default in coordinator mode.

## Registered operations

Every node runs with `acceptSql` at its default, so it accepts no SQL over the network. [`src/operations.ts`](src/operations.ts) registers the four reads and four writes the dashboard uses, and each node serves them by name. `sirannon-codegen` turns that registry into the references the dashboard calls it through:

```sh
pnpm run codegen
```

That writes [`src/generated/operations.ts`](src/generated/operations.ts), which is checked in. Every write declares `fromIdentity`, so the server fills the `audit_log.actor` column from the authenticated caller rather than trusting the request, and each write validates its arguments before it produces a statement.

The dashboard reads join `customers` to `entitlements`, so they stay ordinary reads rather than live queries; a live query maintains a single-table result. Reactivity here comes from CDC table subscriptions, and each one passes `onReset` so that a reconnect landing outside the retained history re-reads the control plane instead of showing rows that are quietly out of date. The [web-client example](../web-client/) shows the live-query form.

## Read routing

Each node advertises the endpoints a client may read from through `GET /db/entitlements/cluster`. A node in the in-sync set advertises `local` and `majority`; a lagging one advertises `local` only, and a faulted, draining, or repairing node is left out entirely. Both clients ask for `readConcern: 'majority'`, so they route away from a node that cannot serve it. `authorizeClusterStatus` limits that route to callers holding the bearer token, so a browser holding only the WebSocket subprotocol cannot read cluster topology.

## Security boundary

This repository example is limited to localhost and is not an authentication template for deployment. It uses a shared local token for HTTP requests and a WebSocket subprotocol token for browser subscriptions:

```sh
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

TanStack server functions validate inputs with Zod and then call a registered write by name; the statements live on the server and never cross the network. A bearer token identifies the `control-plane-operator` actor and the WebSocket subprotocol identifies `control-plane-browser`, and the audit log records whichever one made each change. The browser-visible token exists only to demonstrate authenticated local subscriptions. Replace the shared tokens, restrict origins, and terminate application traffic with TLS before exposing a similar service outside localhost.

The gRPC replication links use mTLS. The example's etcd endpoint uses plain HTTP with `allowInsecure: true` because it stays inside the local Docker network. Production coordinator access requires HTTPS and an authenticated Sirannon identity, either mTLS credentials or etcd username/password authentication.

## Environment

The application accepts these optional variables:

```sh
SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
TOXIPROXY_URL=http://127.0.0.1:8474
VITE_SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

The server-side client uses `SIRANNON_CLUSTER_ENDPOINTS` over HTTP. The browser client uses `VITE_SIRANNON_CLUSTER_ENDPOINTS` over WebSocket for subscriptions.
