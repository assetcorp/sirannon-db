from __future__ import annotations

WORKLOAD_LABELS = {
    "point-select": "Point-select",
    "single-row-insert": "Single-row insert",
    "single-row-update": "Single-row update",
    "bulk-insert": "Single-row insert",
    "batch-update": "Single-row update",
    "ycsb-a": "YCSB-A (50/50 read/update)",
    "ycsb-b": "YCSB-B (95/5 read/update)",
    "ycsb-c": "YCSB-C (read-only)",
    "ycsb-f": "YCSB-F (read-modify-write)",
    "tpc-c-derived": "TPC-C-derived",
}

DURABILITY_LABELS = {
    "full": "Full durability (fsync every commit)",
    "matched": "Matched-relaxed (deferred fsync)",
}

DURABILITY_ORDER = ["full", "matched"]


def workload_label(workload: str) -> str:
    return WORKLOAD_LABELS.get(workload, workload)


def durability_label(durability: str) -> str:
    return DURABILITY_LABELS.get(durability, durability)


def durabilities(comparison: dict) -> list[tuple[str, dict]]:
    node = comparison.get("durabilities", {})
    return [(name, node[name]) for name in DURABILITY_ORDER if name in node]


def run_date(comparison: dict) -> str:
    created = comparison.get("created_at") or ""
    return created[:10] if isinstance(created, str) else ""
