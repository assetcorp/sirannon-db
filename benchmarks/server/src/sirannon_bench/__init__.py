"""Server-versus-server benchmark harness for Sirannon and PostgreSQL.

The harness drives each database through its own shipping client: Sirannon over HTTP into
its real server front-end, and PostgreSQL over its binary socket protocol through psycopg.
Both run co-located in resource-capped containers on one host, so the numbers measure the two
engines doing the same work rather than one skipping a network hop the other pays.
"""

from __future__ import annotations

__version__ = "0.1.0"
