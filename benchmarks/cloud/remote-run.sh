#!/usr/bin/env bash
#
# Run the Sirannon benchmark suite on the VM. The suite drives Sirannon and
# PostgreSQL in resource-capped Docker containers, so a reachable Docker daemon
# is required. The exit status is written to ~/bench.status with ~/bench.done as
# the completion marker the orchestrator watches for.

set -uo pipefail

LABEL="${BENCH_MACHINE_LABEL:-GCP VM}"
REPO="$HOME/sirannon"
status=0

cd "$REPO" || { echo "repo not found at $REPO" >&2; exit 1; }
export BENCH_MACHINE_LABEL="$LABEL"

stamp() { printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# The suite drives resource-capped containers, so the Docker daemon must be
# reachable in this session. usermod adds the login user to the docker group in
# bootstrap, but the group only takes effect in a fresh login; the detached run
# inherits the launching session, so fail fast with a clear message rather than
# a confusing permission error deep in docker compose.
if ! docker ps >/dev/null 2>&1; then
  stamp "docker is not usable in this session; the benchmark suite needs it"
  echo 1 > "$HOME/bench.status"
  touch "$HOME/bench.done"
  exit 1
fi

stamp "Sirannon vs PostgreSQL suite (capped Docker containers)"
( cd benchmarks/server && ./run-all.sh "${BENCH_PROFILE:-cloud}" ) || status=1

stamp "finished with status $status"
echo "$status" > "$HOME/bench.status"
touch "$HOME/bench.done"
exit "$status"
