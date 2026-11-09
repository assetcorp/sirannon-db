from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
ENVIRONMENT = PACKAGE_ROOT / ".venv"
LOCKED_REQUIREMENTS = PACKAGE_ROOT / "requirements-dev.txt"
INSTALLED_DIGEST = ENVIRONMENT / "requirements-dev.sha256"
ON_WINDOWS = os.name == "nt"


def executable(name: str) -> Path:
    folder = "Scripts" if ON_WINDOWS else "bin"
    return ENVIRONMENT / folder / (f"{name}.exe" if ON_WINDOWS else name)


def wanted_digest() -> str:
    digest = hashlib.sha256(LOCKED_REQUIREMENTS.read_bytes())
    digest.update(sys.version.encode())
    digest.update(sys.executable.encode())
    return digest.hexdigest()


def install(digest: str) -> None:
    shutil.rmtree(ENVIRONMENT, ignore_errors=True)
    subprocess.run([sys.executable, "-m", "venv", str(ENVIRONMENT)], check=True)
    subprocess.run(
        [
            str(executable("python")),
            "-m",
            "pip",
            "install",
            "--quiet",
            "--disable-pip-version-check",
            "--require-hashes",
            "--no-deps",
            "--only-binary=:all:",
            "--requirement",
            str(LOCKED_REQUIREMENTS),
        ],
        check=True,
    )
    INSTALLED_DIGEST.write_text(digest, encoding="utf-8")


def ensure_environment() -> None:
    digest = wanted_digest()
    if INSTALLED_DIGEST.is_file() and INSTALLED_DIGEST.read_text(encoding="utf-8") == digest:
        return
    install(digest)


def main() -> int:
    if len(sys.argv) < 2:
        print("Name the program to run, such as ruff, mypy, pytest, or python.", file=sys.stderr)
        return 2
    ensure_environment()
    program, *arguments = sys.argv[1:]
    command = [str(executable("python")), *arguments] if program == "python" else [str(executable(program)), *arguments]
    return subprocess.run(command, cwd=PACKAGE_ROOT, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
