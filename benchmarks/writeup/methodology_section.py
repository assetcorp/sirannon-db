from __future__ import annotations

from comparison_view import durabilities
from render import integer, is_number, table
from run_shape import concurrency_levels, over_network, postgres_docs

SERVER_GUIDE = "https://github.com/assetcorp/sirannon-db/blob/main/docs/server.md#writer-worker"
EXPLICIT_DEFAULT = "The harness sets the default value explicitly."
POSTGRES_DEFAULT_MAX_CONNECTIONS = 100


def _postgres_settings(docs: str) -> list[list[str]]:
    return [
        [
            "PostgreSQL",
            "`shared_buffers`",
            "512MB",
            "128MB",
            "The value is 25% of the 2 GB memory ceiling, which the "
            f"[PostgreSQL documentation]({docs}/runtime-config-resource.html#GUC-SHARED-BUFFERS) gives as a "
            "starting value for a dedicated server.",
        ],
        [
            "PostgreSQL",
            "`work_mem`",
            "64MB",
            "4MB",
            "The harness raises the memory that PostgreSQL gives each sort and hash operation. Every statement in "
            "the benchmark is a key lookup or a single-row write, so none of them sorts or hashes.",
        ],
        [
            "PostgreSQL",
            "`effective_cache_size`",
            "1536MB",
            "4GB",
            "The value is 75% of the 2 GB memory ceiling. The "
            f"[documentation]({docs}/runtime-config-query.html#GUC-EFFECTIVE-CACHE-SIZE) states that the "
            "value should cover the shared buffers and the kernel cache for PostgreSQL data, but it gives no figure.",
        ],
        [
            "PostgreSQL",
            "`maintenance_work_mem`",
            "256MB",
            "64MB",
            f"[Populating a Database]({docs}/populate.html) recommends a larger value while you load large "
            "amounts of data. The harness keeps it for the whole pass.",
        ],
        [
            "PostgreSQL",
            "`wal_level`",
            "minimal",
            "replica",
            f"[Populating a Database]({docs}/populate.html) recommends it while you load large amounts of "
            "data. The harness keeps it for the whole pass, since neither engine replicates in this benchmark.",
        ],
        [
            "PostgreSQL",
            "`max_wal_senders`",
            "0",
            "10",
            f"PostgreSQL [starts with `wal_level=minimal`]({docs}/runtime-config-wal.html#GUC-WAL-LEVEL) "
            "only when this setting is 0.",
        ],
        [
            "PostgreSQL",
            "`checkpoint_timeout`",
            "15min",
            "5min",
            "The harness lengthens the interval so that PostgreSQL checkpoints less often. The "
            f"[documentation]({docs}/runtime-config-wal.html#GUC-CHECKPOINT-TIMEOUT) recommends no value, "
            "and it warns that a longer interval can lengthen crash recovery.",
        ],
        ["PostgreSQL", "`max_wal_size`", "1GB", "1GB", EXPLICIT_DEFAULT],
        [
            "PostgreSQL",
            "`random_page_cost`",
            "1.1",
            "4.0",
            "The harness lowers it for the NVMe disk, so that the planner treats a random read as costing about as "
            "much as a sequential one. The "
            f"[documentation]({docs}/runtime-config-query.html#GUC-RANDOM-PAGE-COST) suggests a lower value "
            "for data that is likely to be completely in cache.",
        ],
    ]


def _max_connections_row(comparison: dict, docs: str) -> list[list[str]]:
    for _, node in durabilities(comparison):
        value = ((node.get("postgres_engine") or {}).get("settings") or {}).get("max_connections")
        if is_number(value):
            return [[
                "PostgreSQL",
                "`max_connections`",
                integer(value),
                integer(POSTGRES_DEFAULT_MAX_CONNECTIONS),
                "The pool opens one connection for each request in flight, so the harness adds the highest "
                f"in-flight limit to the [default]({docs}/runtime-config-connection.html#GUC-MAX-CONNECTIONS).",
            ]]
    return []


_REMAINING_SETTINGS = [
    [
        "PostgreSQL",
        "`statement_timeout`",
        "60000 ms",
        "0, which turns it off",
        "The PostgreSQL driver sets it to the load generator's request timeout.",
    ],
    [
        "Sirannon",
        "`writerWorker`",
        "on",
        "off",
        f"The [server guide]({SERVER_GUIDE}) recommends it so that a disk flush leaves the serving thread free.",
    ],
    [
        "Sirannon",
        "`writerWorker.writeTimeoutMs`",
        "0, which turns it off",
        "30000 ms",
        "In an earlier benchmark, Sirannon reported the writer unresponsive 30 seconds into a `DROP TABLE` between "
        "workloads, because inside the 2 GB memory ceiling that drop can take more than a minute.",
    ],
    [
        "Sirannon",
        "`maxBodyBytes`",
        "1 GiB",
        "1 MiB",
        "The seed load sends 25,000 rows in each request, and a request of that size exceeds 1 MiB.",
    ],
    ["Sirannon", "`readPoolSize`", "4", "4", EXPLICIT_DEFAULT],
    ["Sirannon", "`walMode`", "true", "true", EXPLICIT_DEFAULT],
]


def _settings_table(comparison: dict, docs: str) -> str:
    rows = [*_postgres_settings(docs), *_max_connections_row(comparison, docs), *_REMAINING_SETTINGS]
    return table(
        ["Engine", "Setting", "Value", "Default", "Reason"],
        ["left", "left", "left", "left", "left"],
        rows,
    )


def _transport_sentence(network: bool) -> str:
    if network:
        return (
            "The load generator runs on a machine of its own, and both engines answer it over the private network "
            "between the two machines."
        )
    return "Both engines answer on the same host over loopback."


def _isolation_sentence(network: bool) -> str:
    if network:
        return (
            "The harness gives the engine under test every logical CPU of the engine machine under a hard memory "
            "ceiling, while the load generator has the other machine to itself with no memory cap."
        )
    return (
        "The harness pins the engine "
        "under test to its own logical CPUs under a hard memory ceiling, while the load generator uses separate "
        "logical CPUs with no memory cap."
    )


def _levels_paragraph(comparison: dict) -> str:
    if not concurrency_levels(comparison):
        return ""
    return (
        "\n\nThe harness measures every workload at several limits on the number of requests in flight. At each "
        "limit, it measures every offered rate once. The limit where the engine's operating point is highest "
        "after that first measurement is the engine's peak limit. The harness then measures every rate again at "
        "the peak limit until that limit holds the configured number of runs, and the tables report each engine "
        "at its peak limit. For PostgreSQL, the driver opens one pooled connection for each request that the "
        "limit allows."
    )


def methodology_block(comparison: dict) -> str:
    network = over_network(comparison)
    docs = postgres_docs(comparison)
    return (
        "One Node load generator drives both databases through a small adapter for each one. Sirannon receives "
        "requests from its SDK over the SDK's default WebSocket transport, which sends every concurrent request "
        "over one persistent socket, while PostgreSQL receives them from node-postgres over the PostgreSQL wire "
        f"protocol. {_transport_sentence(network)} The figures include the cost of the JSON "
        "encoding that the Sirannon SDK applies to every call over the WebSocket, because that encoding is part of "
        "Sirannon's client path. Before each sweep, the generator measures each client's throughput ceiling "
        "against the live engine and records it with the results, so that you can see whether a client limited a "
        "rate.\n\n"
        "The two engines handle concurrent requests in different ways. PostgreSQL serves each connection with "
        f"[its own server process]({docs}/tutorial-arch.html), which executes one request at a time. "
        "PostgreSQL can therefore work on as many requests at once as the pool has connections. While "
        "one WAL flush is in progress, PostgreSQL gathers the commits of the other connections that are waiting to "
        "flush and writes them all with the next flush, a mechanism that its documentation calls "
        f"[group commit]({docs}/wal-configuration.html). The Sirannon server executes reads one at a time "
        "on its main thread, which uses its 4 read connections in turn. For writes, its writer thread commits up "
        "to 1,000 queued writes in one transaction, so at full durability one fsync makes the whole group "
        "durable.\n\n"
        "Both engines operate as native processes under Linux cgroup v2 control. "
        f"{_isolation_sentence(network)} The ceiling is smaller than the YCSB table, whose 10,000,000 rows each "
        "have ten 100-character fields. Before it seeds any data, the harness checks at the device level that each "
        "engine's data directory is on the machine's local NVMe. The harness measures the engines one after the "
        "other, starting a fresh engine process for each pass and stopping it afterwards, so neither engine keeps "
        "a warm buffer pool from one pass to the next. The harness also removes Sirannon's data directory after "
        "each Sirannon pass. Between passes, the harness syncs the disks, waits for dirty pages to drain, trims "
        "the data filesystem, and pauses for a fixed interval. Before each pass, it drops the operating system's "
        "page cache, so that neither engine starts with pages that the other engine cached.\n\n"
        "The harness matches durability at two levels. At full durability, PostgreSQL uses "
        "`synchronous_commit=on` and Sirannon uses `synchronous=full`, so both engines fsync every commit. At "
        "matched-relaxed, PostgreSQL uses `synchronous_commit=off` and Sirannon uses `synchronous=normal` in WAL "
        "mode, so both engines defer the fsync. After a power loss, either engine can then lose its most recent "
        "commits, although neither database becomes corrupt.\n\n"
        "The load is open-loop, which means that the generator schedules requests at a fixed target rate whether "
        "or not earlier requests have returned. When the in-flight limit is full, a scheduled request waits for "
        "a free slot. Its latency still counts from the time when the generator was meant to send it, which "
        "corrects for coordinated omission. An engine's operating point is the highest offered rate at which the "
        "engine sustained the load with the p99 of those latencies at or under the target."
        f"{_levels_paragraph(comparison)}\n\n"
        "Each measured window in the sweep is short, so on selected workloads the harness also holds each engine "
        "at its operating point for one long continuous window. That window is long enough to include checkpoints "
        "from both engines, so the 30-second slices that the page reports show whether an engine holds its "
        "operating point while it checkpoints.\n\n"
        "Each engine starts from its defaults. The table below lists every other setting that the harness sets, "
        "except addresses and ports, with its default and the reason for its value.\n\n"
        + _settings_table(comparison, docs)
    )
