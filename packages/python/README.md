# Sirannon Python client

`sirannon-client` connects a Python 3.11 or later program to a [Sirannon](https://sirannon.sondelali.com) server over WebSocket or HTTP. `SirannonClient` blocks until the server answers, and `AsyncSirannonClient` offers the same methods as coroutines for `asyncio`.

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

Every integer comes back as an `int` and every BLOB as `bytes`, including integers beyond 2^53. The client holds a typed method for every HTTP route of the server, and it calls registered operations through the references that `sirannon-codegen --python` writes. After a dropped connection, a change subscription resumes from the last change that it delivered, so no change is missed or repeated while the server keeps the history after that change; once the server has pruned that history, the client calls `on_reset`. Live queries stay current, and the topology-aware client routes reads and writes across a replication group. Every failure raises `SirannonError` with the server's code unchanged.

The [Python client guide](https://github.com/assetcorp/sirannon-db/blob/main/docs/python-client.md) explains each of these.

## Development

`python3 scripts/tools.py` creates a virtual environment from the hashed `requirements-dev.txt` and runs a tool inside it, such as `python3 scripts/tools.py pytest`. `python3 scripts/tools.py python -m generator` checks that the generated models and route methods match `packages/ts/openapi.json` and `packages/ts/asyncapi.json`, and `--write` rewrites them. The tests start the server from `packages/ts/scripts/client-test-server.mjs`, so build `packages/ts` first.
