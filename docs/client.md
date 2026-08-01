# Client SDK

`@delali/sirannon-db/client` mirrors the core `Database` interface over the network, with auto-reconnect and subscription restore on the WebSocket transport.

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

Both calls above name a [registered operation](operations.md), which is what a server serves by default. Send a statement instead once the server runs with `acceptSql: true`:

```ts
const users = await db.query<{ id: number; name: string }>('SELECT * FROM users WHERE active = ?', [1])
await db.execute('INSERT INTO users (name) VALUES (?)', ['Turing'])
```

The client reads `GET /capabilities` once per server and caches the answer. When that answer omits `query.sql`, a statement fails with `SQL_NOT_ACCEPTED` before it leaves the process, so nothing the server would refuse crosses the network.

## Transactions and bulk writes

Both transports carry every write shape. The client sends a whole transaction in one request, and the server commits or rolls it back as a unit, so the client is never in the loop between statements:

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

Change subscriptions and [live queries](live-queries.md) both need the WebSocket transport and fail with `TRANSPORT_ERROR` over HTTP. A subscription reports the rows that changed, and a live query reports the current answer to a registered read.

```ts
const orders = await db.live(ordersByStatus, { status: 'pending' })
orders.subscribe(() => render(orders.getState()))
```

## Topology-aware routing

`@delali/sirannon-db/client/topology` routes between the nodes of a replication group. It reaches internal node addresses, so keep it out of browser bundles. `SirannonClient` fails with `INVALID_ARGUMENT` when you pass it a routing option, which keeps the two apart.

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

Writes always route to the primary, and fail with `NO_SAFE_PRIMARY` when the client holds no current primary. Reads route by preference: `primary` uses the primary, `replica` picks a replica at random, and `nearest` picks the endpoint with the lowest measured round-trip latency.

Static mode uses the `primary` and `replicas` you configured. Coordinator mode treats `endpoints` as a starter list, fetches routing metadata from `GET /db/{id}/cluster`, and caches the current primary, the primary term, and the readable endpoints. That endpoint answers only a credential the server authorises for it, so a client carrying an application credential alone discovers nothing and fails with `ROUTING_ERROR`.

A call that fails with `STALE_PRIMARY`, `AUTHORITY_LOST`, `COORDINATOR_UNAVAILABLE`, `NO_SAFE_PRIMARY`, or `CONNECTION_ERROR` refreshes the routing metadata. A read then retries once on the refreshed route. A write clears its cached transport and raises the error, so a non-idempotent write is never resent without you re-issuing it.

## Read concern

A read concern states how current a read must be, and coordinator mode enforces it. The [replication guide](replication.md#read-concern) defines each level.

```ts
const rows = await db.query(ordersByStatus, { status: 'pending' }, { readConcern: { level: 'linearizable' } })
```

The HTTP and WebSocket transports send a per-call `readConcern` to the server. The topology transport applies the client-level `readConcern` to node selection instead, so it fails a per-call value with `INVALID_ARGUMENT`.

## Transports

The client `Transport` interface carries application queries, writes, and CDC subscriptions over HTTP or WebSocket. It's a separate contract from the `ReplicationTransport` that moves change batches between nodes: `WebSocketTransport` conforms to the first and never the second.

A Node client attaches `headers` to the WebSocket upgrade as well as to HTTP requests, so your `authenticate` hook reads `headers.authorization` on either transport. A browser attaches no header to `new WebSocket(...)`, so carry a short-lived ticket in `webSocketProtocols` there and check it in the same hook. A browser client built with `headers` alone on the WebSocket transport fails at construction with `INVALID_ARGUMENT`, because that credential would never reach the server.

Pass both options when a browser client needs each of them. The topology client sends `headers` on its coordinator discovery request to `GET /db/{id}/cluster`, and the ticket in `webSocketProtocols` on the socket handshake, so a browser reaches an endpoint that reads a bearer token and a socket that reads a subprotocol.

The client offers the plain `sirannon.v1` identifier ahead of the protocols you configure, and the server selects that identifier, so a ticket never comes back in the handshake response.

A server that refuses the upgrade closes with 4401 for an unauthenticated caller and 4403 for a caller it doesn't permit. The client raises `UNAUTHORIZED` or `FORBIDDEN` and leaves that connection closed, because the same credential fails every later attempt. For every other close code it raises `CONNECTION_ERROR` and reconnects while subscriptions remain.

The `ClientOptions` and `TopologyAwareClientOptions` tables are in the [configuration reference](configuration.md).
