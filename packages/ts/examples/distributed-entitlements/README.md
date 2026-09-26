# Run a three-node Sirannon entitlement cluster

This example deploys Sirannon as a three-node, coordinator-backed entitlement control plane. Each node has its own durable SQLite database. Sirannon replicates changes between those databases and uses etcd to manage primary authority and automatic failover.

The application models a SaaS entitlement service. Billing events update customer plans, and usage events decrement quota through idempotent transactions. The dashboard shows the cluster state and the replicated data as they change.

## What runs

- The cluster has three Sirannon data nodes, named `node-a`, `node-b`, and `node-c`.
- One etcd service stores the authority, leases, primary terms, and in-sync set.
- The Sirannon nodes replicate to each other over gRPC, secured with mTLS certificates that the first build generates locally.
- Toxiproxy links between the services let you break the coordinator and replication connections.
- You open the TanStack Start dashboard at `http://127.0.0.1:3001`.

## How traffic moves

The example uses four separate network paths:

| Path | Protocol | Purpose |
| --- | --- | --- |
| TanStack server functions to Sirannon nodes | HTTP | Queries, transactions, and cluster status |
| Browser to Sirannon nodes | WebSocket | Live CDC subscriptions for dashboard refreshes |
| Sirannon node to Sirannon node | gRPC with mTLS | Replication batches, acknowledgements, write forwarding, and first sync |
| Sirannon nodes to etcd | etcd gRPC through Toxiproxy | Primary authority, leases, node sessions, and replication-group metadata |

In this example, applications use WebSocket as a client transport, and the nodes replicate to each other over gRPC. `cluster-node.ts` creates a `GrpcReplicationTransport` for replication, while `direct-client.ts` creates a WebSocket client for browser subscriptions.

Application traffic uses these localhost endpoints:

- `http://127.0.0.1:7301/db/entitlements`
- `http://127.0.0.1:7302/db/entitlements`
- `http://127.0.0.1:7303/db/entitlements`

The containers connect through Toxiproxy links inside the Docker network:

- etcd traffic goes through `toxiproxy:4101`, `toxiproxy:4102`, and `toxiproxy:4103`.
- gRPC replication goes through `toxiproxy:5101`, `toxiproxy:5102`, and `toxiproxy:5103`.

Each node advertises its localhost HTTP endpoint through coordinator discovery. The HTTP and WebSocket clients use those records to route to the current primary and the eligible read replicas after failover.

## Setup

This example needs Node.js 22 or newer, pnpm, and Docker with Compose.

The dashboard imports `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```sh
pnpm install
pnpm --filter @delali/sirannon-db build
```

The code generator behind `pnpm run codegen` imports that same output. The Dockerfile builds the package again for the three nodes, so the next `pnpm run cluster:up` includes any change under `packages/ts/src`. Build the package on your machine again whenever you change the library source.

## Start the example

From this directory, start the example:

```sh
pnpm run dev
```

The `dev` script starts the Docker Compose cluster and the TanStack Start application. The first build creates the local certificate authority and one certificate for each Sirannon node.

The example also has these scripts:

```sh
pnpm run cluster:up
pnpm run cluster:down
pnpm run cluster:reset
pnpm run app:dev
pnpm run typecheck
pnpm run build
```

`cluster:down` and `cluster:reset` remove the example's Docker volumes, including all three SQLite databases and the etcd state.

## Dashboard workflows

- Create a customer and its entitlement record in one transaction.
- Record usage with an idempotency key.
- Replay the same usage event and verify that the quota changes once.
- Apply billing events with monotonic entitlement versions.
- Isolate the current primary from etcd through Toxiproxy and observe failover.
- Restore the coordinator and replication links and watch eligible nodes converge.

Before it sends a write, each server function fetches the cluster status from every node, and it blocks the write unless a majority of the nodes are healthy and report the same primary and term, with that primary among them. Write authority still comes from the coordinator, and in coordinator mode the replication engine uses majority write concern by default.

## Registered operations

Every node leaves `acceptSql` at its default, so no node accepts SQL over the network. [`src/operations.ts`](src/operations.ts) registers the four reads and four writes that the dashboard uses, and each node serves them by name. `sirannon-codegen` turns that registry into the references that the dashboard uses to call it:

```sh
pnpm run codegen
```

That command writes [`src/generated/operations.ts`](src/generated/operations.ts), which the repository tracks. Every write declares `fromIdentity`, so the server fills the `audit_log.actor` column from the authenticated caller's identity. Each write also validates its arguments before it produces a statement.

The dashboard reads all four as ordinary reads and refreshes them whenever a CDC table subscription reports a change. Two of them, `customerEntitlements` and `usageEvents`, join tables, and a live query maintains only a single-table result. A node with `acceptSql` off streams a table's changes only through an `onBeforeSubscribe` hook, so each node registers one that admits the five replicated tables and refuses every other table. Each subscription passes `onReset`, so when a reconnect falls outside the retained change history, the dashboard reads the control plane again and its rows stay current. The [web-client example](../web-client/) shows the live-query form.

## Read routing

Each node lists the endpoints that a client may read from at `GET /db/entitlements/cluster`. The list gives `local` and `majority` for a node in the in-sync set and `local` alone for a lagging node, and it leaves out any node that is faulted, draining, or repairing. Both clients ask for `readConcern: 'majority'`, so they route away from a node that cannot serve it. `authorizeClusterStatus` restricts that route to callers that present the bearer token, so a client with only the WebSocket subprotocol credential cannot read the cluster topology.

## Security boundary

This example serves localhost only, and its authentication is no template for a deployment. It uses a shared local token for HTTP requests and a WebSocket subprotocol token for browser subscriptions:

```sh
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

TanStack server functions validate inputs with Zod and then call a registered write by name, so the statements stay on the server. The bearer token identifies the `control-plane-operator` actor and the WebSocket subprotocol identifies `control-plane-browser`, and the audit log records whichever one made each change. The browser-visible token exists only to demonstrate authenticated local subscriptions. Replace the shared tokens, restrict origins, and terminate application traffic with TLS before you expose a similar service outside localhost.

The gRPC replication links use mTLS. The example's etcd endpoint uses plain HTTP with `allowInsecure: true`, because that traffic stays inside the local Docker network. In production, a node must reach the coordinator over HTTPS with an authenticated identity, through either mTLS credentials or an etcd username and password.

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
