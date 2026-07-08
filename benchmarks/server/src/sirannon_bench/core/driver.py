"""The neutral surface every database implements.

The harness never knows whether it is driving Sirannon or PostgreSQL. It asks a driver to apply
a schema, seed rows, and run a read or a write, and the driver reaches its database through that
database's own shipping client. SQL arrives with ``?`` placeholders; a driver renders the
placeholder in its own style.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from .workloads import SeedTable


class Driver(ABC):
    name: str
    delivery: str
    dialect: str

    @abstractmethod
    async def connect(self) -> None:
        """Open the client and confirm the database is reachable."""

    @abstractmethod
    async def info(self) -> dict:
        """Return the engine version and the durability-relevant settings in force."""

    @abstractmethod
    async def execute_ddl(self, statements: list[str]) -> None:
        """Run each schema statement in order."""

    @abstractmethod
    async def drop_tables(self, tables: list[str]) -> None:
        """Drop each table if it exists, so a workload starts from a known-empty state."""

    @abstractmethod
    async def seed(self, tables: list[SeedTable]) -> None:
        """Bulk-load the seed rows for a workload."""

    @abstractmethod
    async def read(self, sql: str, params: list) -> None:
        """Run one read. The rows are discarded; the caller times the call."""

    @abstractmethod
    async def write(self, sql: str, params: list) -> None:
        """Run one write."""

    @abstractmethod
    async def close(self) -> None:
        """Release the client and its connections."""

    def render(self, sql: str) -> str:
        """Render ``?`` placeholders in the dialect's style. The default keeps ``?``."""
        return sql

    def insert_sql(self, table: SeedTable) -> str:
        placeholders = ", ".join("?" for _ in table.columns)
        columns = ", ".join(table.columns)
        return f"INSERT INTO {table.table} ({columns}) VALUES ({placeholders})"
