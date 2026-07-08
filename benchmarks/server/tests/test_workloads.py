from __future__ import annotations

from sirannon_bench.core.rng import SeededRng, ZipfianGenerator
from sirannon_bench.core.workloads import OperationContext, build_workloads, pick_operation


def test_catalogue_has_the_expected_workloads():
    catalogue = build_workloads()
    assert set(catalogue) == {
        "point-select",
        "bulk-insert",
        "batch-update",
        "ycsb-a",
        "ycsb-b",
        "ycsb-c",
        "ycsb-f",
        "tpc-c-derived",
    }


def test_param_stream_is_deterministic_for_a_seed():
    catalogue = build_workloads()
    operation = catalogue["ycsb-a"].operations[0]

    def stream(seed: int) -> list:
        rng = SeededRng(seed)
        zipf = ZipfianGenerator(1000)
        return [operation.params(OperationContext(rng, zipf, 1000))[0] for _ in range(20)]

    assert stream(42) == stream(42)
    assert stream(42) != stream(7)


def test_pick_operation_honours_weights():
    catalogue = build_workloads()
    operations = catalogue["ycsb-a"].operations
    rng = SeededRng(42)
    reads = sum(1 for _ in range(4000) if pick_operation(rng, operations).kind == "read")
    assert 1800 <= reads <= 2200


def test_seed_produces_expected_row_counts():
    catalogue = build_workloads()
    rng = SeededRng(42)
    tables = catalogue["point-select"].seed(rng, 128)
    assert len(tables) == 1
    assert tables[0].table == "users"
    assert len(tables[0].rows) == 128
    assert len(tables[0].columns) == len(tables[0].rows[0])
