"""The workload catalogue both engines run identically.

Each workload names its schema for each dialect, a seed that fills the table, and a weighted set
of operations. Operations carry SQL for both dialects written with ``?`` placeholders; each
driver renders the placeholder in its own style. Reads, writes, and read-modify-writes are drawn
by weight, so a run reproduces the same operation stream given the same seed.

The set is the standard OLTP core: point-select, bulk-insert, and batch-update as the atomic
operations, YCSB A/B/C/F as the key-value mix, and a TPC-C-shaped transaction mix. YCSB D
(read-latest) and E (short-range-scan) are left out because D needs insert-driven key growth
during the run and E needs ordered-key range scans on a separately loaded table; running a
stated subset is documented, accepted practice.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .rng import SeededRng, ZipfianGenerator

YCSB_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS usertable ("
    "ycsb_key TEXT PRIMARY KEY, "
    "field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, "
    "field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)"
)

MICRO_SCHEMA_SQLITE = (
    "CREATE TABLE IF NOT EXISTS users ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL, bio TEXT)"
)

MICRO_SCHEMA_POSTGRES = (
    "CREATE TABLE IF NOT EXISTS users ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, age INTEGER NOT NULL, bio TEXT)"
)

TPCC_SCHEMA_SQLITE = (
    "CREATE TABLE IF NOT EXISTS customers ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, balance REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS products ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS orders ("
    "id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, "
    "status TEXT NOT NULL, created_at TEXT NOT NULL)"
)

TPCC_SCHEMA_POSTGRES = (
    "CREATE TABLE IF NOT EXISTS customers ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL, balance REAL NOT NULL);"
    "CREATE TABLE IF NOT EXISTS products ("
    "id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, stock INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS orders ("
    "id SERIAL PRIMARY KEY, customer_id INTEGER NOT NULL, total REAL NOT NULL, "
    "status TEXT NOT NULL, created_at TEXT NOT NULL)"
)

_FIRST_NAMES = ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi", "Ivan", "Judy"]
_LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
_FIXED_TIMESTAMP = "2026-01-01T00:00:00.000Z"


@dataclass(frozen=True)
class OperationContext:
    rng: SeededRng
    zipf: ZipfianGenerator
    data_size: int


@dataclass(frozen=True)
class Operation:
    name: str
    weight: float
    kind: str  # "read", "write", or "rmw"
    sqlite_sql: str
    postgres_sql: str
    params: Callable[[OperationContext], list]
    write_sqlite_sql: str | None = None
    write_postgres_sql: str | None = None


@dataclass(frozen=True)
class SeedTable:
    table: str
    columns: tuple[str, ...]
    rows: list[list[object]]


@dataclass(frozen=True)
class Workload:
    name: str
    category: str
    tables: tuple[str, ...]
    sqlite_schema: str
    postgres_schema: str
    seed: Callable[[SeededRng, int], list[SeedTable]]
    operations: tuple[Operation, ...]


def _user_row(rng: SeededRng, row_id: int) -> list[object]:
    name = f"{_FIRST_NAMES[row_id % len(_FIRST_NAMES)]} {_LAST_NAMES[row_id % len(_LAST_NAMES)]}"
    email = f"user{row_id}@example.com"
    age = 18 + (row_id % 62)
    bio = f"Bio for user {row_id}: {rng.text(50)}"
    return [row_id, name, email, age, bio]


def _ycsb_row(rng: SeededRng, key: str) -> list[object]:
    return [key, *[rng.text(100) for _ in range(10)]]


def _customer_row(row_id: int) -> list[object]:
    return [row_id, f"Customer {row_id}", f"customer{row_id}@store.com", 10000.0]


def _product_row(rng: SeededRng, row_id: int) -> list[object]:
    price = round((10 + rng.fraction() * 490) * 100) / 100
    return [row_id, f"Product {row_id}", price, 1000]


def _user_seed(rng: SeededRng, data_size: int) -> list[SeedTable]:
    rows = [_user_row(rng, i + 1) for i in range(data_size)]
    return [SeedTable("users", ("id", "name", "email", "age", "bio"), rows)]


def _empty_seed(rng: SeededRng, data_size: int) -> list[SeedTable]:
    return []


def _ycsb_seed(rng: SeededRng, data_size: int) -> list[SeedTable]:
    columns = ("ycsb_key", *[f"field{i}" for i in range(10)])
    rows = [_ycsb_row(rng, f"user{i}") for i in range(data_size)]
    return [SeedTable("usertable", columns, rows)]


def _tpcc_seed(rng: SeededRng, data_size: int) -> list[SeedTable]:
    customers = [_customer_row(i + 1) for i in range(data_size)]
    product_count = min(data_size, 1000)
    products = [_product_row(rng, i + 1) for i in range(product_count)]
    return [
        SeedTable("customers", ("id", "name", "email", "balance"), customers),
        SeedTable("products", ("id", "name", "price", "stock"), products),
    ]


_YCSB_READ = "SELECT * FROM usertable WHERE ycsb_key = ?"
_YCSB_UPDATE = "UPDATE usertable SET field0 = ? WHERE ycsb_key = ?"


def _ycsb_key(ctx: OperationContext) -> str:
    return f"user{ctx.zipf.next(ctx.rng)}"


def _ycsb_value(ctx: OperationContext) -> str:
    return f"value_{ctx.rng.below(1_000_000)}"


def _ycsb_read_op(weight: float) -> Operation:
    return Operation(
        name="read",
        weight=weight,
        kind="read",
        sqlite_sql=_YCSB_READ,
        postgres_sql=_YCSB_READ,
        params=lambda ctx: [_ycsb_key(ctx)],
    )


def _ycsb_update_op(weight: float) -> Operation:
    return Operation(
        name="update",
        weight=weight,
        kind="write",
        sqlite_sql=_YCSB_UPDATE,
        postgres_sql=_YCSB_UPDATE,
        params=lambda ctx: [_ycsb_value(ctx), _ycsb_key(ctx)],
    )


def _ycsb_rmw_op(weight: float) -> Operation:
    return Operation(
        name="read-modify-write",
        weight=weight,
        kind="rmw",
        sqlite_sql=_YCSB_READ,
        postgres_sql=_YCSB_READ,
        write_sqlite_sql=_YCSB_UPDATE,
        write_postgres_sql=_YCSB_UPDATE,
        params=lambda ctx: [_ycsb_key(ctx), _ycsb_value(ctx)],
    )


def _ycsb_workload(name: str, operations: tuple[Operation, ...]) -> Workload:
    return Workload(
        name=name,
        category="ycsb",
        tables=("usertable",),
        sqlite_schema=YCSB_SCHEMA,
        postgres_schema=YCSB_SCHEMA,
        seed=_ycsb_seed,
        operations=operations,
    )


def _point_select_params(ctx: OperationContext) -> list:
    return [ctx.zipf.next(ctx.rng) + 1]


def _bulk_insert_params(ctx: OperationContext) -> list:
    row_id = ctx.rng.below(ctx.data_size) + 1
    return _user_row(ctx.rng, row_id)


def _batch_update_params(ctx: OperationContext) -> list:
    row_id = ctx.zipf.next(ctx.rng) + 1
    return [ctx.rng.below(80) + 18, row_id]


def _new_order_params(ctx: OperationContext) -> list:
    customer_id = ctx.rng.below(ctx.data_size) + 1
    total = round(ctx.rng.fraction() * 500 * 100) / 100
    return [customer_id, total, "pending", _FIXED_TIMESTAMP]


def _payment_params(ctx: OperationContext) -> list:
    amount = round(ctx.rng.fraction() * 100 * 100) / 100
    customer_id = ctx.rng.below(ctx.data_size) + 1
    return [amount, customer_id]


def build_workloads() -> dict[str, Workload]:
    catalogue: list[Workload] = [
        Workload(
            name="point-select",
            category="micro",
            tables=("users",),
            sqlite_schema=MICRO_SCHEMA_SQLITE,
            postgres_schema=MICRO_SCHEMA_POSTGRES,
            seed=_user_seed,
            operations=(
                Operation(
                    name="point-select",
                    weight=1.0,
                    kind="read",
                    sqlite_sql="SELECT * FROM users WHERE id = ?",
                    postgres_sql="SELECT * FROM users WHERE id = ?",
                    params=_point_select_params,
                ),
            ),
        ),
        Workload(
            name="bulk-insert",
            category="micro",
            tables=("users",),
            sqlite_schema=MICRO_SCHEMA_SQLITE,
            postgres_schema=MICRO_SCHEMA_POSTGRES,
            seed=_empty_seed,
            operations=(
                Operation(
                    name="bulk-insert",
                    weight=1.0,
                    kind="write",
                    sqlite_sql="INSERT OR REPLACE INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?)",
                    postgres_sql=(
                        "INSERT INTO users (id, name, email, age, bio) VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"
                    ),
                    params=_bulk_insert_params,
                ),
            ),
        ),
        Workload(
            name="batch-update",
            category="micro",
            tables=("users",),
            sqlite_schema=MICRO_SCHEMA_SQLITE,
            postgres_schema=MICRO_SCHEMA_POSTGRES,
            seed=_user_seed,
            operations=(
                Operation(
                    name="batch-update",
                    weight=1.0,
                    kind="write",
                    sqlite_sql="UPDATE users SET age = ? WHERE id = ?",
                    postgres_sql="UPDATE users SET age = ? WHERE id = ?",
                    params=_batch_update_params,
                ),
            ),
        ),
        _ycsb_workload("ycsb-a", (_ycsb_read_op(0.5), _ycsb_update_op(0.5))),
        _ycsb_workload("ycsb-b", (_ycsb_read_op(0.95), _ycsb_update_op(0.05))),
        _ycsb_workload("ycsb-c", (_ycsb_read_op(1.0),)),
        _ycsb_workload("ycsb-f", (_ycsb_read_op(0.5), _ycsb_rmw_op(0.5))),
        Workload(
            name="tpc-c-derived",
            category="oltp",
            tables=("orders", "products", "customers"),
            sqlite_schema=TPCC_SCHEMA_SQLITE,
            postgres_schema=TPCC_SCHEMA_POSTGRES,
            seed=_tpcc_seed,
            operations=(
                Operation(
                    name="new-order",
                    weight=0.5,
                    kind="write",
                    sqlite_sql="INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)",
                    postgres_sql="INSERT INTO orders (customer_id, total, status, created_at) VALUES (?, ?, ?, ?)",
                    params=_new_order_params,
                ),
                Operation(
                    name="payment",
                    weight=0.5,
                    kind="write",
                    sqlite_sql="UPDATE customers SET balance = balance - ? WHERE id = ?",
                    postgres_sql="UPDATE customers SET balance = balance - ? WHERE id = ?",
                    params=_payment_params,
                ),
            ),
        ),
    ]
    return {workload.name: workload for workload in catalogue}


def pick_operation(rng: SeededRng, operations: tuple[Operation, ...]) -> Operation:
    draw = rng.fraction()
    cumulative = 0.0
    for operation in operations:
        cumulative += operation.weight
        if draw < cumulative:
            return operation
    return operations[-1]
