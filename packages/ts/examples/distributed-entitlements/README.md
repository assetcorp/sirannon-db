# Start a three-node Sirannon entitlement cluster

In this example, Docker Compose deploys Sirannon as a three-node entitlement control plane that etcd coordinates. Each node has its own durable SQLite database. Sirannon replicates changes between those databases and uses etcd to manage primary authority and automatic failover.

The application is a model of a SaaS entitlement service. It applies each billing event to the plan of a customer, and it decrements the quota for each usage event through an idempotent transaction. The dashboard shows the cluster state and the replicated data as they change.

## The services

- The cluster has three Sirannon data nodes, named `node-a`, `node-b`, and `node-c`.
- One etcd service stores the authority, leases, primary terms, and in-sync set.
- The Sirannon nodes replicate to each other over gRPC, secured with mTLS certificates that the `certs` service generates on the first start.
- With the Toxiproxy links between the services, you can break the coordinator and replication connections.
- The TanStack Start dashboard is at `http://127.0.0.1:3001`.

## Network paths

The example has four separate network paths:

| Path | Protocol | Purpose |
| --- | --- | --- |
| TanStack server functions to Sirannon nodes | HTTP | Queries, transactions, and cluster status |
| Browser to Sirannon nodes | WebSocket | Live CDC subscriptions for dashboard refreshes |
| Sirannon node to Sirannon node | gRPC with mTLS | Replication batches, acknowledgements, write forwarding, and first sync |
| Sirannon nodes to etcd | etcd gRPC through Toxiproxy | Primary authority, leases, node sessions, and replication-group metadata |

In this example, applications use WebSocket as a client transport, and the nodes replicate to each other over gRPC. The code in `cluster-node.ts` creates a `GrpcReplicationTransport` for replication, while the code in `direct-client.ts` creates a WebSocket client for browser subscriptions.

Applications connect to these localhost endpoints:

- `http://127.0.0.1:7301/db/entitlements`
- `http://127.0.0.1:7302/db/entitlements`
- `http://127.0.0.1:7303/db/entitlements`

The containers connect through Toxiproxy links inside the Docker network:

- The nodes connect to etcd through `toxiproxy:4101`, `toxiproxy:4102`, and `toxiproxy:4103`.
- The nodes replicate over gRPC through `toxiproxy:5101`, `toxiproxy:5102`, and `toxiproxy:5103`.

Each node advertises its localhost HTTP endpoint through coordinator discovery. The HTTP and WebSocket clients use those records to route to the current primary and the eligible read replicas after failover.

## Setup

To start this example, you need Node.js 22 or newer, pnpm, and Docker with Compose.

The dashboard imports `@delali/sirannon-db` from the workspace. That import resolves to files under `packages/ts/dist`, so build the package before you start anything. From the repository root:

```sh
pnpm install
pnpm --filter @delali/sirannon-db build
```

The code generator behind `pnpm run codegen` imports that same output. Docker builds the package again from `Dockerfile.node` for the three nodes, so the images from the next `pnpm run cluster:up` contain any change under `packages/ts/src`. Build the package on your machine again whenever you change the library source.

## Start the example

From this directory, start the example:

```sh
pnpm run dev
```

The `dev` script starts the Docker Compose cluster and the TanStack Start application. On the first start, the `certs` service creates the local certificate authority and one certificate for each Sirannon node.

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

Before it sends a write, each server function fetches the cluster status from every node. It sends the write only when a majority of the nodes are healthy and report the same primary and term, and that primary is one of them. The coordinator still grants write authority, and in coordinator mode, the replication engine uses majority write concern by default.

## Registered operations

`acceptSql` stays at its default on every node, so no node executes SQL from the network. The registry in [`src/operations.ts`](src/operations.ts) contains the four reads and four writes of the dashboard, and each node serves them by name. From that registry, `sirannon-codegen` generates the typed references for the dashboard:

```sh
pnpm run codegen
```

That command writes [`src/generated/operations.ts`](src/generated/operations.ts), which git tracks. Every write declares `fromIdentity`, so the server fills the `audit_log.actor` column from the identity of the authenticated caller. Each write also validates its arguments before it produces a statement.

The dashboard calls all four reads as ordinary reads, and it calls them again whenever its CDC table subscription receives a change. It uses ordinary reads because two of them, `customerEntitlements` and `usageEvents`, join tables, and Sirannon updates a live query in place only when the query selects from one table. When `acceptSql` is off, a node streams the changes of a table only when an `onBeforeSubscribe` hook is registered, so each node registers a hook that throws for every table outside the five replicated tables. Each subscription passes `onReset`, so when a reconnect falls outside the retained change history, the dashboard reads the control plane again. The [web-client example](../web-client/) uses the live-query form.

## Read routing

At `GET /db/entitlements/cluster`, each node lists the endpoints that a client may read from. The response marks a node in the in-sync set with `local` and `majority`, and a lagging node with `local` alone. It omits any node that is faulted, draining, or repairing. Both clients set `readConcern: 'majority'`, so they send reads only to a node that can serve majority reads. `authorizeClusterStatus` restricts that route to callers that present the bearer token, so a client with only the WebSocket subprotocol credential cannot read the cluster topology.

## Security boundary

This example serves localhost only, so keep its authentication out of any deployment. It uses a shared local token for HTTP requests and a WebSocket subprotocol token for browser subscriptions:

```sh
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

TanStack server functions validate inputs with Zod and then call a registered write by name, so the statements stay on the server. The `authenticate` hook maps the bearer token to the `control-plane-operator` actor and the WebSocket subprotocol token to `control-plane-browser`. The audit log stores the actor of each change. The example exposes a token to the browser only for authenticated local subscriptions. Replace the shared tokens, restrict origins, and terminate application traffic with TLS before you expose a similar service outside localhost.

The gRPC replication links use mTLS. The example's etcd endpoint uses plain HTTP with `allowInsecure: true`, because that traffic stays inside the local Docker network. In production, a node must connect to the coordinator over HTTPS with an authenticated identity, through either mTLS credentials or an etcd username and password.

## Environment

The application reads these optional variables:

```sh
SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
TOXIPROXY_URL=http://127.0.0.1:8474
VITE_SIRANNON_CLUSTER_ENDPOINTS=http://127.0.0.1:7301,http://127.0.0.1:7302,http://127.0.0.1:7303
VITE_SIRANNON_CLUSTER_TOKEN=sirannon-entitlements-local-token
```

The server-side client uses `SIRANNON_CLUSTER_ENDPOINTS` over HTTP. The browser client uses `VITE_SIRANNON_CLUSTER_ENDPOINTS` over WebSocket for subscriptions.
