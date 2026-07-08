"""Drive PostgreSQL through its native binary socket protocol with psycopg.

PostgreSQL is reached the way an application reaches it: a pooled binary-protocol connection over
the socket, one transaction per write in autocommit mode to match Sirannon's per-statement
commit. The pool size is disclosed, because the pool is where PostgreSQL's per-connection cost
shows up as the client count climbs. Durability is set per session: ``synchronous_commit = on``
for the full-durability pass and ``off`` for the matched-relaxed pass, so the fsync behaviour
matches Sirannon's ``synchronous = FULL`` and ``NORMAL`` respectively.
"""

from __future__ import annotations

from psycopg_pool import AsyncConnectionPool

from ..core.config import PostgresConfig
from ..core.driver import Driver
from ..core.workloads import SeedTable

_SYNCHRONOUS_COMMIT = {"full": "on", "matched": "off"}


class PostgresDriver(Driver):
    name = "postgres"
    delivery = "socket"
    dialect = "postgres"

    def __init__(self, config: PostgresConfig, durability: str) -> None:
        self._config = config
        self._durability = durability
        self._synchronous_commit = _SYNCHRONOUS_COMMIT.get(durability, "off")
        self._pool: AsyncConnectionPool | None = None

    def _conninfo(self) -> str:
        cfg = self._config
        return (
            f"host={cfg.host} port={cfg.port} user={cfg.user} "
            f"password={cfg.password} dbname={cfg.database}"
        )

    async def connect(self) -> None:
        synchronous_commit = self._synchronous_commit

        async def configure(conn) -> None:
            await conn.set_autocommit(True)
            await conn.execute(f"SET synchronous_commit = {synchronous_commit}")

        pool = AsyncConnectionPool(
            self._conninfo(),
            min_size=1,
            max_size=self._config.pool_size,
            open=False,
            configure=configure,
        )
        await pool.open(wait=True)
        self._pool = pool

    def _pool_or_raise(self) -> AsyncConnectionPool:
        if self._pool is None:
            raise RuntimeError("Postgres driver used before connect()")
        return self._pool

    def render(self, sql: str) -> str:
        return sql.replace("?", "%s")

    async def info(self) -> dict:
        async with self._pool_or_raise().connection() as conn:
            version = (await (await conn.execute("SELECT version()")).fetchone())[0]
            sync = (await (await conn.execute("SHOW synchronous_commit")).fetchone())[0]
        return {
            "engine": "postgres",
            "delivery": "socket",
            "durability_requested": self._durability,
            "synchronous_commit": str(sync),
            "version": str(version),
        }

    async def execute_ddl(self, statements: list[str]) -> None:
        async with self._pool_or_raise().connection() as conn:
            for statement in statements:
                await conn.execute(statement)

    async def drop_tables(self, tables: list[str]) -> None:
        async with self._pool_or_raise().connection() as conn:
            for table in tables:
                await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")

    async def seed(self, tables: list[SeedTable]) -> None:
        async with self._pool_or_raise().connection() as conn:
            for table in tables:
                if not table.rows:
                    continue
                insert = self.render(self.insert_sql(table))
                async with conn.transaction():
                    async with conn.cursor() as cursor:
                        await cursor.executemany(insert, table.rows)

    async def read(self, sql: str, params: list) -> None:
        async with self._pool_or_raise().connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(self.render(sql), params)
                await cursor.fetchall()

    async def write(self, sql: str, params: list) -> None:
        async with self._pool_or_raise().connection() as conn:
            await conn.execute(self.render(sql), params)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
