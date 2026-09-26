# Client SDK

`@delali/sirannon-db/client` offers the core `Database` interface over the network. On the WebSocket transport, the client reconnects automatically and restores its subscriptions.

```ts
import { SirannonClient } from '@delali/sirannon-db/client'
import { operationRef } from '@delali/sirannon-db'

const ordersByStatus = operationRef<{ status: string }, { id: number; total: number }>('ordersByStatus')
const placeOrder = operationRef<{ total: number }>('placeOrder')

const client = new SirannonClient('http://localhost:9876', { transport: 'websocket', autoReconnect: true })
const db = client.database('app')

const orders = await db.query(ordersByStatus, { status: 'pending' })
await db.execute(placeOrder, { total: 4999 })

const sub = await db.on('orders').subscribe(event => console.log('Order changed:', event))

sub.unsubscribe()
client.close()
```

Both calls above name a [registered operation](operations.md), which is what a server serves by default. Send a statement instead once the server sets `acceptSql: true`:

```ts
const users = await db.query<{ id: number; name: string }>('SELECT * FROM users WHERE active = ?', [1])
await db.execute('INSERT INTO users (name) VALUES (?)', ['Turing'])
```

The client reads `GET /capabilities` once per server and caches the answer. When that answer omits `query.sql`, a statement fails with `SQL_NOT_ACCEPTED` before it leaves the process, so the client never sends a statement that the server would refuse.

## Transactions and bulk writes

Both transports support every write shape. The client sends a whole transaction in one request, and the server commits or rolls it back as a unit, so no network round trip falls between two of its statements:

```ts
await db.transaction([
  { sql: 'UPDATE accounts SET balance = balance - 50 WHERE id = ?', params: [1] },
  { sql: 'UPDATE accounts SET balance = balance + 50 WHERE id = ?', params: [2] },
])

await db.batch('INSERT INTO tags (label) VALUES (?)', [['sqlite'], ['realtime']])

await db.loadAll('INSERT INTO events (id, payload) VALUES (?, ?)', rowStream, { batchSize: 5000 })
```

`loadAll` splits an iterable into batches, sends each as one request, and checkpoints the WAL once at the end. Each batch must fit under the server's `maxBodyBytes`.

## Subscriptions and live queries

Change subscriptions and [live queries](live-queries.md) both need the WebSocket transport and fail with `TRANSPORT_ERROR` over HTTP. A subscription reports the rows that changed, while a live query reports the current answer to a registered read.

```ts
const orders = await db.live(ordersByStatus, { status: 'pending' })
orders.subscribe(() => render(orders.getState()))
```

## Topology-aware routing

`@delali/sirannon-db/client/topology` routes between the nodes of a replication group. It connects to internal node addresses, so keep it out of browser bundles. `SirannonClient` fails with `INVALID_ARGUMENT` when you pass it a routing option, so routing stays in the topology client.

```ts
import { TopologyAwareClient } from '@delali/sirannon-db/client/topology'

const client = new TopologyAwareClient({
  endpoints: ['https://node-a.internal', 'https://node-b.internal'],
  discovery: 'coordinator',
  readPreference: 'nearest',
  readConcern: 'majority',
  headers: { authorization: `Bearer ${process.env.SIRANNON_TOPOLOGY_TOKEN}` },
})
```

The client always routes a write to the primary, and in coordinator mode it fails the write with `NO_SAFE_PRIMARY` when the routing metadata names no current primary. The client routes a read by preference: `primary` uses the primary, `replica` picks a replica at random, and `nearest` picks the endpoint with the lowest measured round-trip latency.

Static mode uses the `primary` and `replicas` that you configure. Coordinator mode treats `endpoints` as a starter list, fetches routing metadata from `GET /db/{id}/cluster`, and caches the current primary, the primary term, and the readable endpoints. The server answers that route only for a credential that it authorises there, so a client with only an application credential discovers nothing and fails with `ROUTING_ERROR`.

In coordinator mode, when a call fails with `STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, or `CONNECTION_ERROR`, the client refreshes the routing metadata. It then retries a read once on the refreshed route. For a write, it clears the cached transport and raises the error, so the client never sends a write a second time unless you issue it again.

When the client cannot reach the endpoint for a read, it sets that endpoint aside for 5,000 ms in both modes. In static mode, it then retries the read once on another endpoint, unless the endpoint that failed is the primary.

## Read concern

A read concern states how current a read must be, and the replication engine enforces it in coordinator mode. The [replication guide](replication.md#read-concern) defines each level.

```ts
const rows = await db.query(ordersByStatus, { status: 'pending' }, { readConcern: { level: 'linearizable' } })
```

The HTTP and WebSocket transports send a per-call `readConcern` to the server. The topology transport applies the client-level `readConcern` to node selection, so it fails a per-call value with `INVALID_ARGUMENT`.

## Transports

The client `Transport` interface defines the calls that send application queries, writes, and CDC subscriptions over HTTP or WebSocket. It is separate from `ReplicationTransport`, which moves change batches between nodes, and `WebSocketTransport` implements only the client interface.

A Node client attaches `headers` to the WebSocket upgrade as well as to HTTP requests, so your `authenticate` hook reads `headers.authorization` on either transport. A browser attaches no header to `new WebSocket(...)`, so for a browser, put a short-lived ticket in `webSocketProtocols` and check it in the same hook. When you build a browser client on the WebSocket transport with `headers` alone, the constructor fails with `INVALID_ARGUMENT`, because the browser would never send that credential to the server.

Pass both options when a browser client needs each of them. The topology client sends `headers` on its coordinator discovery request to `GET /db/{id}/cluster`, and the ticket in `webSocketProtocols` on the socket handshake, so a browser can reach both a discovery route that expects a bearer token and a socket that expects a subprotocol ticket.

The client offers the plain `sirannon.v1` identifier ahead of the protocols that you configure, and the server selects that identifier, so the handshake response never echoes a ticket.

When a server refuses the upgrade, it closes with 4401 for an unauthenticated caller and 4403 for a caller that it doesn't permit. The client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed, because the same credential would fail every later attempt. For every other close code, it raises `CONNECTION_ERROR` and reconnects as long as any subscription remains open.

The `ClientOptions` and `TopologyAwareClientOptions` tables are in the [configuration reference](configuration.md).
