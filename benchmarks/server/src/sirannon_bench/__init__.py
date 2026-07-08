"""Aggregation for the Sirannon-versus-PostgreSQL benchmark.

A Node load generator drives each database through its own shipping client: Sirannon over its
SDK's WebSocket transport, and PostgreSQL over its binary socket protocol through node-postgres.
The generator writes one result file per engine and durability level. This package joins those
files into the cross-engine comparison the writeup renders, so aggregation stays in Python while
the measured load path runs in the language of the client under test.
"""

from __future__ import annotations

__version__ = "0.1.0"
