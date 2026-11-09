from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import typing
from pathlib import Path
from typing import Any

import pytest

from sirannon_client import ReadRef, WriteRef
from tests.servers import REPOSITORY

CODEGEN = REPOSITORY / "packages" / "ts" / "dist" / "codegen" / "cli.mjs"
CODEGEN_TIMEOUT_SECONDS = 60
MYPY_TIMEOUT_SECONDS = 300
REGISTRY_MODULE = """import { readFileSync } from 'node:fs'
const spec = JSON.parse(readFileSync(new URL('./registry.json', import.meta.url), 'utf8'))
const build = group => ({
  reads: Object.fromEntries(Object.entries(group.reads).map(([name, read]) => [name, {
    args: read.args,
    ...(read.columns === null ? {} : { columns: read.columns }),
    statement: () => ({ sql: 'SELECT * FROM items' }),
  }])),
  writes: Object.fromEntries(Object.entries(group.writes).map(([name, write]) => [name, {
    args: write.args,
    statements: () => ({ sql: 'DELETE FROM items' }),
  }])),
})
export const operations = Object.fromEntries(Object.entries(spec.databases).map(([id, group]) => [id, build(group)]))
export const sharedOperations = build(spec.shared)
"""
HOSTILE_REGISTRY: dict[str, Any] = {
    "databases": {
        "items": {
            "reads": {
                "itemById": {"args": ["id"], "columns": ["id", "label"]},
                "class": {
                    "args": ["from", "__x", "a b", "ключ", 'quo"te', '"""', "\U0001f600"],
                    "columns": ["Any", "def", "ok", " "],
                },
            },
            "writes": {"__secret": {"args": ["id"]}, "\U0001f600": {"args": []}},
        },
        "9-lives": {"reads": {"naïve": {"args": [], "columns": None}}, "writes": {}},
        "émoji-db": {"reads": {}, "writes": {"ok": {"args": ["x", "x"]}}},
        'tri"""ple': {"reads": {}, "writes": {"mark": {"args": ["id"]}}},
    },
    "shared": {
        "reads": {"listAll": {"args": [], "columns": ["id"]}, "constructor": {"args": ["id"], "columns": ["id"]}},
        "writes": {"with space": {"args": ["v"]}},
    },
}
COLLIDING_REGISTRY: dict[str, Any] = {
    "databases": {},
    "shared": {
        "reads": {"with space": {"args": [], "columns": ["id"]}, "with_space": {"args": [], "columns": ["id"]}},
        "writes": {},
    },
}

Entry = tuple[str, tuple[str, ...], tuple[str, ...] | None]
Group = tuple[tuple[Entry, ...], tuple[Entry, ...]]


def run_codegen(*arguments: str) -> subprocess.CompletedProcess[str]:
    node = shutil.which("node")
    assert node is not None
    command = [node, str(CODEGEN), *arguments]
    return subprocess.run(command, capture_output=True, text=True, timeout=CODEGEN_TIMEOUT_SECONDS, check=False)


def generate(directory: Path, registry: dict[str, Any]) -> subprocess.CompletedProcess[str]:
    (directory / "registry.json").write_text(json.dumps(registry), encoding="utf-8")
    (directory / "registry.mjs").write_text(REGISTRY_MODULE, encoding="utf-8")
    return run_codegen(
        "--registry",
        str(directory / "registry.mjs"),
        "--python",
        str(directory / "hostile_operations.py"),
        "--manifest",
        str(directory / "manifest.json"),
    )


def load(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


def keys_of(hint: Any) -> tuple[str, ...] | None:
    if not typing.is_typeddict(hint):
        return None
    return tuple(sorted(hint.__required_keys__))


def generated_group(namespace: type) -> Group:
    kinds: list[tuple[Entry, ...]] = []
    for kind in ("Reads", "Writes"):
        holder = getattr(namespace, kind)
        hints = typing.get_type_hints(holder)
        entries: list[Entry] = []
        for attribute, hint in hints.items():
            ref = getattr(holder, attribute)
            assert isinstance(ref, (ReadRef, WriteRef))
            arguments = typing.get_args(hint)
            args = keys_of(arguments[0])
            assert args is not None
            entries.append((ref.name, args, keys_of(arguments[1]) if isinstance(ref, ReadRef) else None))
        kinds.append(tuple(sorted(entries)))
    return kinds[0], kinds[1]


def sorted_keys(keys: list[str] | None) -> tuple[str, ...] | None:
    return None if keys is None else tuple(sorted(set(keys)))


def expected_group(group: dict[str, Any]) -> Group:
    reads = tuple(
        sorted(
            (name, tuple(sorted(set(read["args"]))), sorted_keys(read["columns"]))
            for name, read in group["reads"].items()
        )
    )
    writes = tuple(sorted((name, tuple(sorted(set(write["args"]))), None) for name, write in group["writes"].items()))
    return reads, writes


def test_generated_python_references_carry_every_hostile_operation_name_argument_and_column(tmp_path: Path) -> None:
    generated = generate(tmp_path, HOSTILE_REGISTRY)
    assert generated.returncode == 0, generated.stderr
    output = tmp_path / "hostile_operations.py"
    assert output.read_text(encoding="utf-8").isascii()

    checked = subprocess.run(
        [sys.executable, "-m", "mypy", "--strict", "--cache-dir", str(tmp_path / ".mypy"), str(output)],
        capture_output=True,
        text=True,
        timeout=MYPY_TIMEOUT_SECONDS,
        check=False,
    )
    assert checked.returncode == 0, checked.stdout

    module = load(output)
    namespaces = [value for value in vars(module).values() if isinstance(value, type) and hasattr(value, "Reads")]
    groups = [*HOSTILE_REGISTRY["databases"].values(), HOSTILE_REGISTRY["shared"]]
    assert sorted(generated_group(namespace) for namespace in namespaces) == sorted(map(expected_group, groups))

    from_manifest = tmp_path / "from_manifest.py"
    regenerated = run_codegen("--from-manifest", str(tmp_path / "manifest.json"), "--python", str(from_manifest))
    assert regenerated.returncode == 0, regenerated.stderr
    assert from_manifest.read_bytes() == output.read_bytes()


def test_codegen_refuses_two_operations_that_generate_one_identifier_and_leaves_the_old_file(tmp_path: Path) -> None:
    output = tmp_path / "hostile_operations.py"
    output.write_text("previous = True\n", encoding="utf-8")

    generated = generate(tmp_path, COLLIDING_REGISTRY)

    assert generated.returncode != 0
    assert "with space" in generated.stderr
    assert "with_space" in generated.stderr
    assert output.read_text(encoding="utf-8") == "previous = True\n"


@pytest.fixture(autouse=True)
def forget_generated_modules() -> typing.Iterator[None]:
    yield
    sys.modules.pop("hostile_operations", None)
