"""Map an engine name to its driver."""

from __future__ import annotations

from .config import Config
from .driver import Driver


def build_driver(engine: str, config: Config, durability: str) -> Driver:
    if engine == "sirannon":
        from ..drivers.sirannon import SirannonHttpDriver

        return SirannonHttpDriver(
            base_url=config.sirannon.base_url,
            database_id=config.sirannon.database_id,
            durability=durability,
            max_in_flight=config.max_in_flight,
        )
    if engine == "postgres":
        from ..drivers.postgres import PostgresDriver

        return PostgresDriver(config.postgres, durability)
    raise ValueError(f"unknown engine {engine!r}; expected 'sirannon' or 'postgres'")
