# Python client

`sirannon-client` connects a Python 3.11 or later program to a Sirannon server over WebSocket or HTTP. It comes in two forms that share one implementation: `SirannonClient` for synchronous code, and `AsyncSirannonClient` for `asyncio`. Both forms have a typed method for every HTTP route that the server describes in `openapi.json`, so when you generate the client again, it gains every route that the server has added.

```bash
pip install sirannon-client
```

```python
from sirannon_client import SirannonClient

with SirannonClient("http://localhost:9876") as client:
    db = client.database("orders")
    db.execute("INSERT INTO orders (id, total) VALUES (?, ?)", [42, 4999])
    rows = db.query("SELECT id, total FROM orders WHERE id = ?", [42])
```

```python
from sirannon_client import AsyncSirannonClient

async with AsyncSirannonClient("http://localhost:9876") as client:
    db = client.database("orders")
    rows = await db.query("SELECT id, total FROM orders WHERE id = ?", [42])
```

Every method of `SirannonClient` blocks until the server answers, while the method of the same name on `AsyncSirannonClient` is a coroutine. The client returns every integer as a Python `int`, including an integer beyond 2^53, and every BLOB as `bytes`, so you read back each value exactly as SQLite stores it. It sends both types the same way.

## Registered operations

A server refuses statements unless it sets `acceptSql: true`, so a program that talks to such a server calls the [registered operations](operations.md) that the server defines. `sirannon-codegen` writes a typed reference for each of them:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --python ./app/operations.py
```

A Python project that has no access to the server's source can generate the same file from the manifest that `sirannon-codegen --manifest` writes during the server's build:

```bash
npx -p @delali/sirannon-db sirannon-codegen --from-manifest ./operations.json --python ./app/operations.py
```

The generator writes a class for each database, with nested `Reads` and `Writes` classes. Each operation becomes a `ReadRef` or a `WriteRef` whose arguments and rows are `TypedDict` types:

```python
from app.operations import OrdersOperations

orders = OrdersOperations.Reads

rows = db.query(orders.orderById({"id": 42}))
db.execute(OrdersOperations.Writes.placeOrder({"id": 43, "total": 4999}))
```

Calling a reference with its arguments lets mypy check every key of the dict against the operation, so a misspelt argument fails the type check. `db.query(orders.orderById, {"id": 42})` sends the same request, but mypy checks only the row type in that form. When the server leaves `query.sql` out of its capabilities, the client refuses a statement with `SQL_NOT_ACCEPTED` before sending it, while every registered operation keeps working.

## Subscriptions and live queries

Over the WebSocket transport, which is the default, the client delivers each change to a table once, in the order of the server's change log:

```python
def on_change(event):
    print(event.type, event.row, event.seq)

def on_reset():
    print("read the table again")

subscription = db.on("orders").filter({"status": "pending"}).subscribe(on_change, on_reset=on_reset)
```

When the connection drops, the client reconnects every `reconnect_interval_ms` and resumes from the last change that it delivered. While the server keeps the history after that change, your callback receives every change exactly once. When the server has pruned that history, the client calls `on_reset`, so read the table again in that function. When the server refuses the connection with close code 4401 or 4403, the client stops reconnecting and passes that error to `on_error`.

`SirannonClient` calls every callback on one delivery thread, in the order in which the server sent the changes. While 1024 callbacks are waiting, the client pauses reading from the connection until they catch up. If a slow callback keeps the client paused long enough, the server can close the connection with code 4290. The client then reconnects and resumes from the last change that it delivered.

A live query holds the rows of a registered read, and the client updates them as the table changes:

```python
live = db.live(orders.pendingOrders)
live.subscribe(lambda update: print(update.state.rows))
live.close()
```

The client applies the server's edits in the order in which the server sent them. After a reconnection, it subscribes again and replaces the rows with a fresh result.

## Replication groups

`TopologyAwareClient` and `AsyncTopologyAwareClient` send writes to the primary of a [replication group](replication.md) and send reads to the node that matches `read_preference`:

```python
from sirannon_client import TopologyAwareClient

with TopologyAwareClient(
    primary="https://db-1.example.com",
    replicas=["https://db-2.example.com", "https://db-3.example.com"],
    read_preference="replica",
) as client:
    rows = client.database("orders").query(orders.orderById({"id": 42}))
```

`read_preference` takes `primary`, `replica`, or `nearest`, and with `nearest` the client measures each node's latency. With `discovery="coordinator"` and a list of `endpoints`, the client fetches the group's routing from `GET /db/{id}/cluster` and sends calls to the new primary after a failover. When the client cannot reach the node for a read, it sets that node aside for five seconds and sends the read once more to another node. When a write fails, the client raises the error without sending the write again, so a write reaches the server twice only when your program sends it twice.

## Errors

The client raises every failure as a `SirannonError`, whose `code`, `message`, `details`, and HTTP `status` hold what the server sent, unchanged. `ErrorCodes` lists the codes that the client raises when it fails before the server answers, such as `SQL_NOT_ACCEPTED`, `CONNECTION_ERROR`, and `TIMEOUT`. The [error reference](errors.md) lists the codes that the server sends.

## Routes

The client has a method for every route in `openapi.json`, which covers backups, encryption, device sync, health, and capabilities, alongside the database methods above:

```python
health = client.get_health()
capabilities = client.get_capabilities()
```

Each method takes the route's path parameters and request body and returns a model of its response. The client has no device sync controller, so a program that syncs a device calls the device sync routes itself.
