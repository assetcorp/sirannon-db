#!/usr/bin/env bash
# Run the Sirannon-versus-Postgres comparison end to end.
#
# For every durability level the script drives each engine through its own shipping client from a
# co-located load-driver container, one engine at a time, then aggregates the run and regenerates
# the page. Cold start is timed for both engines as the interval from container start to the first
# successful in-container health probe. Everything lands under results/runs/<run id>/.
#
# Usage:
#   ./run-all.sh                 # same as cloud
#   ./run-all.sh cloud           # full-scale run: 10,000,000 rows, both durabilities, regenerates the page
#   ./run-all.sh smoke           # fast throwaway check: 10,000 rows, one durability, no page, self-cleaning
#
# A preset only fills in defaults; any BENCH_ variable exported by hand still overrides it. A smoke
# run keeps its output under results/.smoke (git-ignored) and leaves the published page untouched.

set -uo pipefail
cd "$(dirname "$0")"

PROFILE="${1:-${BENCH_PROFILE:-cloud}}"
SMOKE=""
case "${PROFILE}" in
  cloud)
    : "${BENCH_DATA_SIZE:=10000000}"
    : "${BENCH_DURABILITIES:=full matched}"
    : "${BENCH_RUNS:=5}"
    : "${BENCH_WARMUP_SECONDS:=3}"
    : "${BENCH_MEASURE_SECONDS:=10}"
    : "${BENCH_TARGET_RATES:=1000,4000,16000}"
    : "${BENCH_PASS_TIMEOUT:=5400}"
    ;;
  smoke)
    SMOKE=1
    : "${BENCH_DATA_SIZE:=10000}"
    : "${BENCH_DURABILITIES:=matched}"
    : "${BENCH_RUNS:=2}"
    : "${BENCH_WARMUP_SECONDS:=1}"
    : "${BENCH_MEASURE_SECONDS:=3}"
    : "${BENCH_TARGET_RATES:=1000,4000}"
    : "${BENCH_PASS_TIMEOUT:=1200}"
    ;;
  *)
    echo "unknown profile '${PROFILE}' (expected: cloud or smoke)" >&2
    exit 2
    ;;
esac
export BENCH_DATA_SIZE BENCH_DURABILITIES BENCH_RUNS BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_TARGET_RATES
export BENCH_PASS_TIMEOUT

BENCH_RUN_ID="${BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
export BENCH_RUN_ID
DURABILITIES="${BENCH_DURABILITIES:-full matched}"
COLD_START_TIMEOUT="${BENCH_COLD_START_TIMEOUT:-120}"

# A smoke run is isolated under a git-ignored results tree so it is never committed or taken as the
# latest run. The bench container writes it through its /app path; the host reaches the same mount
# as ./results.
if [ -n "${SMOKE}" ]; then
  HOST_RESULTS_DIR="results/.smoke"
  export BENCH_RESULTS_DIR="/app/results/.smoke"
else
  HOST_RESULTS_DIR="results"
fi
RUN_DIR="${HOST_RESULTS_DIR}/runs/${BENCH_RUN_ID}"

echo "profile: ${PROFILE}"
echo "run id: ${BENCH_RUN_ID}"
echo "durabilities: ${DURABILITIES}"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

compose() { docker compose "$@"; }

DEADLINE_BIN=""
if command -v timeout >/dev/null 2>&1; then
  DEADLINE_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  DEADLINE_BIN="gtimeout"
fi

# Run a command under a wall-clock deadline so a stalled pass can never hang the run. Uses
# coreutils timeout when present (the cloud image has it) and a portable background watcher
# otherwise. On the deadline it kills the command, removes any leftover ephemeral bench
# container, and returns 124 so the caller records the pass as a failure and moves on.
run_with_deadline() {
  local secs="$1"
  shift
  local rc=0
  if [ -n "$DEADLINE_BIN" ]; then
    "$DEADLINE_BIN" -k 30 "$secs" "$@"
    rc=$?
  else
    "$@" &
    local cmd_pid=$!
    (
      waited=0
      while [ "$waited" -lt "$secs" ]; do
        kill -0 "$cmd_pid" 2>/dev/null || exit 0
        sleep 2
        waited=$((waited + 2))
      done
      kill -TERM "$cmd_pid" 2>/dev/null
      sleep 30
      kill -KILL "$cmd_pid" 2>/dev/null
    ) &
    local watcher=$!
    wait "$cmd_pid" 2>/dev/null
    rc=$?
    kill "$watcher" 2>/dev/null
    wait "$watcher" 2>/dev/null
  fi
  if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ] || [ "$rc" -eq 143 ]; then
    echo "pass exceeded its ${secs}s deadline; killed it and cleaning up the container" >&2
    docker rm -f $(docker ps -aq --filter "name=bench-run") >/dev/null 2>&1 || true
    return 124
  fi
  return "$rc"
}

wait_probe() {
  # Poll an in-container probe command until it succeeds, then print the elapsed milliseconds
  # from the supplied start time. Prints nothing and returns non-zero on timeout.
  local service="$1" start_ms="$2" deadline
  shift 2
  deadline=$(( $(now_ms) + COLD_START_TIMEOUT * 1000 ))
  while [ "$(now_ms)" -lt "$deadline" ]; do
    if compose exec -T "$service" "$@" >/dev/null 2>&1; then
      echo $(( $(now_ms) - start_ms ))
      return 0
    fi
    sleep 0.05
  done
  return 1
}

cold_start_ms=""
measure_cold_start() {
  local service="$1"
  shift
  local start elapsed
  start="$(now_ms)"
  compose up -d --force-recreate "$service" >/dev/null 2>&1
  if elapsed="$(wait_probe "$service" "$start" "$@")"; then
    cold_start_ms="$elapsed"
  else
    cold_start_ms=""
  fi
}

echo "building images"
compose build || exit 1

echo "measuring cold start"
measure_cold_start postgres pg_isready -q -U benchmark
pg_cold="${cold_start_ms}"
measure_cold_start sirannon node -e "fetch('http://127.0.0.1:9876/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"
sirannon_cold="${cold_start_ms}"

mkdir -p "$RUN_DIR"
python3 - "$RUN_DIR/cold-start.json" "$sirannon_cold" "$pg_cold" <<'PY'
import json
import sys

path, sirannon_cold, pg_cold = sys.argv[1], sys.argv[2], sys.argv[3]
definition = "Milliseconds from the container start command to the first successful in-container health probe."
payload = {"definition": definition}
if sirannon_cold:
    payload["sirannon"] = {"cold_start_ms": int(sirannon_cold)}
if pg_cold:
    payload["postgres"] = {"cold_start_ms": int(pg_cold)}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

compose up -d postgres >/dev/null 2>&1

status=0
first=1
for durability in $DURABILITIES; do
  echo "================ durability: ${durability} ================"
  BENCH_DURABILITY="$durability" compose up -d --force-recreate sirannon >/dev/null 2>&1 || status=1

  sirannon_args=(--engine sirannon --durability "$durability")
  if [ "$first" = "1" ]; then
    sirannon_args+=(--features)
  fi
  run_with_deadline "$BENCH_PASS_TIMEOUT" docker compose run --rm bench "${sirannon_args[@]}" || status=1
  run_with_deadline "$BENCH_PASS_TIMEOUT" docker compose run --rm bench --engine postgres --durability "$durability" || status=1
  first=0
done

echo "================ aggregate ================"
run_with_deadline 600 docker compose run --rm aggregate || status=1

if [ -n "${SMOKE}" ]; then
  echo "================ writeup (skipped for smoke) ================"
else
  echo "================ writeup ================"
  python3 ../writeup/generate.py || status=1
fi

echo "================ teardown ================"
compose down -v >/dev/null 2>&1 || true

if [ -n "${SMOKE}" ]; then
  echo "smoke results kept at ${RUN_DIR} (git-ignored); remove with: rm -rf ${HOST_RESULTS_DIR}"
fi

echo "finished with status ${status}"
exit "$status"
