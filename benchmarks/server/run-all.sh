#!/usr/bin/env bash
# Run the Sirannon-versus-Postgres comparison end to end.
#
# For every durability level the script drives each engine through its own shipping client from a
# co-located load-driver container, one engine at a time, then aggregates the run and regenerates
# the page. Cold start is timed for both engines as the interval from container start to the first
# successful in-container health probe. Everything lands under results/runs/<run id>/.
#
# Usage:
#   ./run-all.sh                                  # both durabilities, default workloads
#   BENCH_DURABILITIES=matched ./run-all.sh       # one durability only
#   BENCH_WORKLOADS=point-select BENCH_TARGET_RATES=1000 BENCH_RUNS=2 \
#     BENCH_WARMUP_SECONDS=1 BENCH_MEASURE_SECONDS=2 ./run-all.sh   # a quick smoke

set -uo pipefail
cd "$(dirname "$0")"

BENCH_RUN_ID="${BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
export BENCH_RUN_ID
DURABILITIES="${BENCH_DURABILITIES:-full matched}"
RUN_DIR="results/runs/${BENCH_RUN_ID}"
COLD_START_TIMEOUT="${BENCH_COLD_START_TIMEOUT:-120}"

echo "run id: ${BENCH_RUN_ID}"
echo "durabilities: ${DURABILITIES}"

now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

compose() { docker compose "$@"; }

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
  compose run --rm bench "${sirannon_args[@]}" || status=1
  compose run --rm bench --engine postgres --durability "$durability" || status=1
  first=0
done

echo "================ aggregate ================"
compose run --rm --entrypoint python bench -m sirannon_bench.aggregate || status=1

echo "================ writeup ================"
python3 ../writeup/generate.py || status=1

echo "================ teardown ================"
compose down -v >/dev/null 2>&1 || true

echo "finished with status ${status}"
exit "$status"
