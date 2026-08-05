#!/usr/bin/env bash
# shellcheck source-path=SCRIPTDIR

set -uo pipefail
cd "$(dirname "$0")" || exit 1

. ./lib/preflight.sh
. ./lib/units.sh
. ./lib/cooldown.sh
. ./lib/engines.sh

PROFILE="${1:-${BENCH_PROFILE:-cloud}}"
SMOKE=""
case "${PROFILE}" in
  cloud)
    : "${BENCH_DATA_SIZE:=10000000}"
    : "${BENCH_DURABILITIES:=full matched}"
    : "${BENCH_RUNS:=5}"
    : "${BENCH_WARMUP_SECONDS:=3}"
    : "${BENCH_MEASURE_SECONDS:=10}"
    : "${BENCH_TARGET_RATES:=1000,2000,4000,8000,16000,32000,64000}"
    : "${BENCH_SOAK_SECONDS:=1200}"
    : "${BENCH_PASS_TIMEOUT:=14400}"
    ;;
  smoke)
    SMOKE=1
    : "${BENCH_DATA_SIZE:=10000}"
    : "${BENCH_DURABILITIES:=matched}"
    : "${BENCH_RUNS:=2}"
    : "${BENCH_WARMUP_SECONDS:=1}"
    : "${BENCH_MEASURE_SECONDS:=3}"
    : "${BENCH_TARGET_RATES:=1000,4000,16000,64000}"
    : "${BENCH_SOAK_SECONDS:=30}"
    : "${BENCH_PASS_TIMEOUT:=1800}"
    ;;
  *)
    echo "unknown profile '${PROFILE}' (expected: cloud or smoke)" >&2
    exit 2
    ;;
esac
: "${BENCH_WRITE_TIMEOUT_MS:=0}"
: "${BENCH_SCHEMA_TIMEOUT_MS:=1800000}"
export BENCH_DATA_SIZE BENCH_DURABILITIES BENCH_RUNS BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_TARGET_RATES
export BENCH_SOAK_SECONDS BENCH_PASS_TIMEOUT BENCH_WRITE_TIMEOUT_MS BENCH_SCHEMA_TIMEOUT_MS
export PYTHONUNBUFFERED=1
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0

require_supported_host

DRIVER_CPUSET="${BENCH_DRIVER_CPUSET:-0-3}"
ENGINE_CPUSET="${BENCH_ENGINE_CPUSET:-4-7}"
ENGINE_MEMORY="$(printf '%s' "${BENCH_ENGINE_MEMORY:-2G}" | tr '[:lower:]' '[:upper:]')"
if [ -n "${BENCH_DRIVER_MEMORY:-}" ]; then
  echo "note: the load driver runs without a memory cap; BENCH_DRIVER_MEMORY is ignored"
fi
derived_driver_cpus="$(cpuset_count "$DRIVER_CPUSET")"
derived_engine_cpus="$(cpuset_count "$ENGINE_CPUSET")"
if [ -n "${BENCH_DRIVER_CPUS:-}" ] && [ "$BENCH_DRIVER_CPUS" != "$derived_driver_cpus" ]; then
  echo "note: BENCH_DRIVER_CPUS is derived from BENCH_DRIVER_CPUSET (${DRIVER_CPUSET} -> ${derived_driver_cpus}); the supplied value is ignored"
fi
if [ -n "${BENCH_ENGINE_CPUS:-}" ] && [ "$BENCH_ENGINE_CPUS" != "$derived_engine_cpus" ]; then
  echo "note: BENCH_ENGINE_CPUS is derived from BENCH_ENGINE_CPUSET (${ENGINE_CPUSET} -> ${derived_engine_cpus}); the supplied value is ignored"
fi
BENCH_DRIVER_CPUS="$derived_driver_cpus"
BENCH_ENGINE_CPUS="$derived_engine_cpus"
export BENCH_DRIVER_CPUS BENCH_ENGINE_CPUS
export BENCH_DRIVER_CPUSET="$DRIVER_CPUSET" BENCH_ENGINE_CPUSET="$ENGINE_CPUSET" BENCH_ENGINE_MEMORY="$ENGINE_MEMORY"

BENCH_PG_PASSWORD="${BENCH_PG_PASSWORD:-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')}"
export BENCH_PG_PASSWORD

SSD_MODE="${BENCH_LOCAL_SSD_MODE:-auto}"
if [ -z "${BENCH_DATA_ROOT:-}" ]; then
  if findmnt -no TARGET /mnt/nvme >/dev/null 2>&1; then
    BENCH_DATA_ROOT="/mnt/nvme/bench"
  else
    BENCH_DATA_ROOT="/var/lib/sirannon-bench"
  fi
fi
PG_DATA_DIR="$BENCH_DATA_ROOT/pg"
SIRANNON_DATA_DIR="$BENCH_DATA_ROOT/sirannon"

COOLDOWN_SECONDS="${BENCH_COOLDOWN_SECONDS:-30}"
COOLDOWN_DIRTY_KB="${BENCH_COOLDOWN_DIRTY_KB:-16384}"
COOLDOWN_TIMEOUT="${BENCH_COOLDOWN_TIMEOUT:-120}"
COLD_START_TIMEOUT="${BENCH_COLD_START_TIMEOUT:-120}"

BENCH_RUN_ID="${BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
export BENCH_RUN_ID
DURABILITIES="${BENCH_DURABILITIES:-full matched}"
ENGINES="${BENCH_ENGINES:-sirannon postgres}"

engine_enabled() {
  case " $ENGINES " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

for engine_name in $ENGINES; do
  case "$engine_name" in
    sirannon | postgres) ;;
    *)
      echo "unknown engine '${engine_name}' in BENCH_ENGINES (expected: sirannon, postgres)" >&2
      exit 2
      ;;
  esac
done

if [ -n "${SMOKE}" ]; then
  HOST_RESULTS_DIR="results/.smoke"
else
  HOST_RESULTS_DIR="results"
fi
BENCH_RESULTS_DIR="$(pwd)/${HOST_RESULTS_DIR}"
export BENCH_RESULTS_DIR
RUN_DIR="${BENCH_RESULTS_DIR}/runs/${BENCH_RUN_ID}"

RUN_AS_USER="$(id -un)"
NODE_BIN="$(command -v node)"
REPO_ROOT="$(cd ../.. && pwd)"
DRIVER_DIR="$(pwd)/driver"

echo "profile: ${PROFILE}"
echo "run id: ${BENCH_RUN_ID}"
echo "durabilities: ${DURABILITIES}"
echo "engines: ${ENGINES}"
echo "data root: ${BENCH_DATA_ROOT} (local SSD mode: ${SSD_MODE})"
echo "driver cores: ${DRIVER_CPUSET}; engine cores: ${ENGINE_CPUSET}; engine memory ceiling: ${ENGINE_MEMORY}"

trap cleanup_units EXIT
cleanup_units
confine_login_sessions

echo "building the SDK and installing driver dependencies"
(cd "$REPO_ROOT" && pnpm install --frozen-lockfile && pnpm --filter @delali/sirannon-db build) || exit 1
(cd "$DRIVER_DIR" && pnpm install --ignore-workspace --frozen-lockfile) || exit 1

mkdir -p "$RUN_DIR" || { echo "could not create ${RUN_DIR}" >&2; exit 1; }
sudo install -d -o "$RUN_AS_USER" "$BENCH_DATA_ROOT" || { echo "could not create ${BENCH_DATA_ROOT}" >&2; exit 1; }
sudo install -d -o "$RUN_AS_USER" "$SIRANNON_DATA_DIR" || { echo "could not create ${SIRANNON_DATA_DIR}" >&2; exit 1; }
sudo rm -rf "$PG_DATA_DIR" || { echo "could not clear ${PG_DATA_DIR}" >&2; exit 1; }
sudo install -d -m 700 -o postgres -g postgres "$PG_DATA_DIR" || { echo "could not create ${PG_DATA_DIR}" >&2; exit 1; }

if ! sudo -u postgres test -x "$BENCH_DATA_ROOT"; then
  echo "the postgres system user cannot traverse ${BENCH_DATA_ROOT}; set BENCH_DATA_ROOT outside any private home directory" >&2
  exit 1
fi

check_data_device "$PG_DATA_DIR" || exit 1
check_data_device "$SIRANNON_DATA_DIR" || exit 1
init_postgres || { echo "initdb failed" >&2; exit 1; }

echo "measuring cold start"
start="$(now_ms)"
start_postgres || exit 1
pg_cold="$(wait_probe "$start" pg_probe)" || pg_cold=""
verify_engine_cgroup bench-postgres.service || exit 1
PGPASSWORD="$BENCH_PG_PASSWORD" "$PG_BINDIR/createdb" -h 127.0.0.1 -p 5432 -U benchmark benchmark || exit 1
unit_stop bench-postgres.service

start="$(now_ms)"
start_sirannon matched || exit 1
sirannon_cold="$(wait_probe "$start" sirannon_probe)" || sirannon_cold=""
verify_engine_cgroup bench-sirannon.service || exit 1
unit_stop bench-sirannon.service

python3 - "$RUN_DIR/cold-start.json" "$sirannon_cold" "$pg_cold" <<'PY'
import json
import sys

path, sirannon_cold, pg_cold = sys.argv[1], sys.argv[2], sys.argv[3]
definition = "Milliseconds from the process start command to the first successful health probe."
payload = {"definition": definition}
if sirannon_cold:
    payload["sirannon"] = {"cold_start_ms": int(sirannon_cold)}
if pg_cold:
    payload["postgres"] = {"cold_start_ms": int(pg_cold)}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY

status=0
read -r -a DURABILITY_LIST <<< "$DURABILITIES"
total="${#DURABILITY_LIST[@]}"
index=0
for durability in "${DURABILITY_LIST[@]}"; do
  echo "================ durability: ${durability} ================"
  if engine_enabled sirannon; then
    sirannon_args=(--engine sirannon --durability "$durability")
    if [ "$index" -eq 0 ]; then
      sirannon_args+=(--features)
    fi
    if start_sirannon "$durability" \
      && wait_probe "$(now_ms)" sirannon_probe >/dev/null \
      && verify_engine_cgroup bench-sirannon.service; then
      drop_caches
      BENCH_ENGINE_CGROUP="/sys/fs/cgroup/system.slice/bench-sirannon.service" \
        run_driver_pass "${sirannon_args[@]}" || status=1
      record_engine_caps_proof bench-sirannon.service sirannon "$durability"
    else
      echo "sirannon is not healthy under the verified caps; skipping its ${durability} pass" >&2
      status=1
    fi
    unit_stop bench-sirannon.service
    rm -rf "$SIRANNON_DATA_DIR"
    cooldown
  fi

  if engine_enabled postgres; then
    if start_postgres \
      && wait_probe "$(now_ms)" pg_probe >/dev/null \
      && verify_engine_cgroup bench-postgres.service; then
      drop_caches
      BENCH_ENGINE_CGROUP="/sys/fs/cgroup/system.slice/bench-postgres.service" \
        run_driver_pass --engine postgres --durability "$durability" || status=1
      record_engine_caps_proof bench-postgres.service postgres "$durability"
    else
      echo "postgres is not healthy under the verified caps; skipping its ${durability} pass" >&2
      status=1
    fi
    unit_stop bench-postgres.service
  fi
  index=$((index + 1))
  if [ "$index" -lt "$total" ]; then
    cooldown
  fi
done

echo "================ aggregate ================"
run_with_deadline 600 env PYTHONPATH=src python3 -m sirannon_bench.aggregate || status=1

if [ -n "${SMOKE}" ]; then
  echo "================ writeup (skipped for smoke) ================"
else
  echo "================ writeup ================"
  python3 ../writeup/generate.py || status=1
fi

echo "================ teardown ================"
unit_stop bench-postgres.service
unit_stop bench-sirannon.service
sudo rm -rf "$PG_DATA_DIR" "$SIRANNON_DATA_DIR"

if [ -n "${SMOKE}" ]; then
  echo "smoke results kept at ${RUN_DIR} (git-ignored); remove with: rm -rf ${HOST_RESULTS_DIR}"
fi

echo "finished with status ${status}"
exit "$status"
