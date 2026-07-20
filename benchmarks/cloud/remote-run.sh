#!/usr/bin/env bash

set -uo pipefail

LABEL="${BENCH_MACHINE_LABEL:-GCP VM}"
REPO="$HOME/sirannon"
status=0

cd "$REPO" || { echo "repo not found at $REPO" >&2; exit 1; }
export BENCH_MACHINE_LABEL="$LABEL"

stamp() { printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

fail_early() {
  stamp "$1"
  echo 1 > "$HOME/bench.status"
  touch "$HOME/bench.done"
  exit 1
}

command -v systemd-run >/dev/null 2>&1 || fail_early "systemd-run is missing; the suite pins engines with systemd cgroups"
[ -x /usr/lib/postgresql/17/bin/postgres ] || fail_early "PostgreSQL 17 is missing; run the setup step (remote-bootstrap.sh) first"
command -v node >/dev/null 2>&1 || fail_early "Node is missing; run the setup step (remote-bootstrap.sh) first"
command -v pnpm >/dev/null 2>&1 || fail_early "pnpm is missing; run the setup step (remote-bootstrap.sh) first"

stamp "Sirannon vs PostgreSQL suite (native engines under cgroup caps)"
( cd benchmarks/server && ./run-all.sh "${BENCH_PROFILE:-cloud}" ) || status=1

stamp "finished with status $status"
echo "$status" > "$HOME/bench.status"
touch "$HOME/bench.done"
exit "$status"
