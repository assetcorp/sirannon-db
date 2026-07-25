# Client SDK

`@delali/sirannon-db/client` mirrors the core `Database` interface over the network, with auto-reconnect and subscription restore on the WebSocket transport.

```ts
import { SirannonClient } from '@delali/sirannon-db/client'

const client = new SirannonClient('http://localhost:9876', { transport: 'websocket', autoReconnect: true })
const db = client.database('app')

const users = await db.query<{ id: number; name: string }>('SELECT * FROM users')
await db.execute('INSERT INTO users (name) VALUES (?)', ['Turing'])

const sub = await db.on('users').subscribe(event => console.log('User changed:', event))

sub.unsubscribe()
client.close()
```

Transactions use the HTTP transport:

```ts
const httpDb = new SirannonClient('http://localhost:9876', { transport: 'http' }).database('app')

await httpDb.transaction([
  { sql: 'UPDATE accounts SET balance = balance - 50 WHERE id = ?', params: [1] },
  { sql: 'UPDATE accounts SET balance = balance + 50 WHERE id = ?', params: [2] },
])
```

The client `Transport` interface carries application queries, writes, and CDC subscriptions over HTTP or WebSocket. It is separate from the `ReplicationTransport` that moves change batches between nodes: `WebSocketTransport` conforms to the first and never the second.

Browsers cannot attach an `Authorization` header to `new WebSocket(...)`, so pass a short-lived value through `webSocketProtocols` and check it in the server's `onRequest` hook. The `ClientOptions` table is in the [configuration reference](configuration.md).
