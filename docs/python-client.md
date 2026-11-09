# Python client

`sirannon-client` connects a Python 3.11 or later program to a Sirannon server over WebSocket or HTTP. It comes in two forms that share one implementation: `SirannonClient` for ordinary code, and `AsyncSirannonClient` for `asyncio`. Both forms hold a typed method for every HTTP route that the server describes in `openapi.json`, so a route that the server gains appears in the client when the client is generated again.

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

Every method of `SirannonClient` blocks until the server answers, and the method of the same name on `AsyncSirannonClient` is a coroutine. The client returns every integer as a Python `int`, including an integer beyond 2^53, and every BLOB as `bytes`, so a value reads back exactly as SQLite stores it. It sends both types the same way.

## Registered operations

A server refuses statements unless it sets `acceptSql: true`, so most programs call the [registered operations](operations.md) that the server defines. `sirannon-codegen` writes a typed reference for each of them:

```bash
pnpm exec sirannon-codegen --registry ./src/operations.ts --python ./app/operations.py
```

A Python project that has no access to the server's source can generate the same file from the manifest that the server's build writes with `--manifest`:

```bash
npx -p @delali/sirannon-db sirannon-codegen --from-manifest ./operations.json --python ./app/operations.py
```

Each database becomes a class with `Reads` and `Writes`, and each operation becomes a `ReadRef` or a `WriteRef` whose arguments and rows are `TypedDict` types:

```python
from app.operations import OrdersOperations

orders = OrdersOperations.Reads

rows = db.query(orders.orderById({"id": 42}))
db.execute(OrdersOperations.Writes.placeOrder({"id": 43, "total": 4999}))
```

Calling a reference with its arguments lets mypy check every key of the dict against the operation, so a misspelt argument fails the type check. `db.query(orders.orderById, {"id": 42})` sends the same request, but mypy checks only the row type in that form. When the server leaves `query.sql` out of its capabilities, the client refuses a statement with `SQL_NOT_ACCEPTED` before sending it, while every registered operation keeps working.

## Subscriptions and live queries

The WebSocket transport, which is the default, delivers each change to a table once and in the order of the server's change log:

```python
def on_change(event):
    print(event.type, event.row, event.seq)

def on_reset():
    print("read the table again")

subscription = db.on("orders").filter({"status": "pending"}).subscribe(on_change, on_reset=on_reset)
```

When the connection drops, the client reconnects every `reconnect_interval_ms` and resumes from the last change that it delivered, so your callback receives no change twice and misses none while the server keeps the history. When the server has pruned that history, the client calls `on_reset`, so read the table again in that function. A server that refuses the connection with close code 4401 or 4403 stops the reconnection, and the client passes that error to `on_error`. `SirannonClient` calls every callback on one delivery thread, in the order that the changes arrived. While 1024 callbacks are waiting to run, the client stops reading from the connection until they catch up. If a slow callback holds the client back long enough, the server can close the connection with code 4290, and the client then reconnects and resumes from the last change that it delivered.

A live query keeps the rows of a registered read current as the table changes:

```python
live = db.live(orders.pendingOrders)
live.subscribe(lambda update: print(update.state.rows))
live.close()
```

The client applies the server's edits in the order that the server sent them. After a reconnection it subscribes again and replaces the rows with a fresh result.

## Replication groups

`TopologyAwareClient` and `AsyncTopologyAwareClient` send writes to the primary of a [replication group](replication.md) and reads to the node that `read_preference` picks:

```python
from sirannon_client import TopologyAwareClient

with TopologyAwareClient(
    primary="https://db-1.example.com",
    replicas=["https://db-2.example.com", "https://db-3.example.com"],
    read_preference="replica",
) as client:
    rows = client.database("orders").query(orders.orderById({"id": 42}))
```

`read_preference` takes `primary`, `replica`, or `nearest`, which measures each node's latency. With `discovery="coordinator"` and a list of `endpoints`, the client reads the group's routing from `GET /db/{id}/cluster` and follows a failover. When a read cannot reach its node, the client sets that node aside for five seconds and runs the read once more on another node. When a write fails, the client refreshes its routing and raises the error, so a write reaches the server twice only when your program sends it twice.

## Errors

Every failure raises `SirannonError`, which carries the server's `code`, `message`, `details`, and HTTP `status` unchanged. `ErrorCodes` lists the codes that the client raises before it reaches a server, such as `SQL_NOT_ACCEPTED`, `CONNECTION_ERROR`, and `TIMEOUT`. The [error reference](errors.md) lists the codes that the server sends.

## Routes

The client has a method for every route in `openapi.json`, which covers backups, encryption, device sync, health, and capabilities, alongside the database methods above:

```python
health = client.get_health()
capabilities = client.get_capabilities()
```

Each method takes the route's path parameters and request body and returns a model of its response. The client has no device sync controller, so a program that syncs a device calls the device sync routes itself.
