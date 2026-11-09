# Sirannon Python client

`sirannon-client` connects a Python 3.11 or later program to a [Sirannon](https://sirannon.sondelali.com) server over WebSocket or HTTP. `SirannonClient` blocks until the server answers, while `AsyncSirannonClient` has the same methods as coroutines for `asyncio`.

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

The client returns every integer as an `int` and every BLOB as `bytes`, including integers beyond 2^53. It has a typed method for every HTTP route of the server, and it calls registered operations through the references that `sirannon-codegen --python` writes.

When a connection drops, a change subscription resumes from the last change that it delivered. While the server keeps the history after that change, the subscription delivers each change once, in order; once the server prunes that history, the client calls `on_reset`. A live query updates its rows as the table changes. `TopologyAwareClient` sends writes to the primary of a replication group and sends reads to the nodes that its `read_preference` selects. The client raises `SirannonError` for every failure, with the server's code unchanged.

The [Python client guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/python-client.md) explains each of these.

## Development

`python3 scripts/tools.py` creates a virtual environment from the hashed `requirements-dev.txt` and starts a tool inside it, such as `python3 scripts/tools.py pytest`. `python3 scripts/tools.py python -m generator` checks that the generated models and route methods match `packages/ts/openapi.json` and `packages/ts/asyncapi.json`, and `--write` rewrites them. The tests start the server from `packages/ts/scripts/client-test-server.mjs`, so build `packages/ts` first.
