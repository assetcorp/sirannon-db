#!/usr/bin/env bash

set -uo pipefail
cd "$(dirname "$0")" || exit 1

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
export BENCH_DATA_SIZE BENCH_DURABILITIES BENCH_RUNS BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_TARGET_RATES
export BENCH_SOAK_SECONDS BENCH_PASS_TIMEOUT
export PYTHONUNBUFFERED=1
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0

if [ "$(uname -s)" != "Linux" ] || ! command -v systemd-run >/dev/null 2>&1; then
  echo "this harness pins engines with systemd cgroups and needs Linux with systemd-run; run it on the benchmark VM through benchmarks/cloud/" >&2
  exit 2
fi
if [ -z "${EPOCHREALTIME:-}" ]; then
  echo "bash 5 or newer is required; EPOCHREALTIME drives the millisecond timers" >&2
  exit 2
fi
for controller in cpuset memory; do
  if ! grep -qw "$controller" /sys/fs/cgroup/cgroup.controllers 2>/dev/null; then
    echo "the cgroup v2 ${controller} controller is unavailable; the resource caps cannot be enforced" >&2
    exit 2
  fi
done
PG_BINDIR="/usr/lib/postgresql/17/bin"
for required in "$PG_BINDIR/postgres" "$PG_BINDIR/initdb" "$PG_BINDIR/pg_isready" "$PG_BINDIR/createdb"; do
  if [ ! -x "$required" ]; then
    echo "missing ${required}; install PostgreSQL 17 first (benchmarks/cloud/remote-bootstrap.sh does this on the VM)" >&2
    exit 2
  fi
done
for required in node pnpm python3 numfmt; do
  if ! command -v "$required" >/dev/null 2>&1; then
    echo "missing ${required}; install it first (benchmarks/cloud/remote-bootstrap.sh does this on the VM)" >&2
    exit 2
  fi
done
if ! sudo -n true 2>/dev/null; then
  echo "sudo needs a password on this host; expect prompts (the benchmark VM grants passwordless sudo)"
fi

CPUSET_PY='
import sys


def expand(spec):
    cpus = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            low, high = part.split("-", 1)
            cpus.update(range(int(low), int(high) + 1))
        else:
            cpus.add(int(part))
    return cpus


if sys.argv[1] == "count":
    print(len(expand(sys.argv[2])))
else:
    sys.exit(0 if expand(sys.argv[2]) == expand(sys.argv[3]) else 1)
'

cpuset_count() { python3 -c "$CPUSET_PY" count "$1"; }

same_cpuset() { python3 -c "$CPUSET_PY" same "$1" "$2"; }

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
# Must stay outside any home: the postgres user traverses every component, and mode 700 blocks it.
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
echo "data root: ${BENCH_DATA_ROOT} (local SSD mode: ${SSD_MODE})"
echo "driver cores: ${DRIVER_CPUSET}; engine cores: ${ENGINE_CPUSET}; engine memory ceiling: ${ENGINE_MEMORY}"

now_ms() {
  local us="${EPOCHREALTIME//[!0-9]/}"
  printf '%s\n' $(( us / 1000 ))
}

# Reusing a unit name while the old one is still deactivating fails with "unit already exists".
unit_stop() {
  local unit="$1" waited=0 state
  sudo systemctl stop "$unit" >/dev/null 2>&1 || true
  while :; do
    state="$(systemctl show -p ActiveState --value "$unit" 2>/dev/null || echo inactive)"
    case "$state" in
      active|activating|deactivating|reloading) ;;
      *) break ;;
    esac
    if [ "$waited" -eq 30 ]; then
      sudo systemctl kill -s SIGKILL "$unit" >/dev/null 2>&1 || true
    fi
    if [ "$waited" -ge 40 ]; then
      echo "${unit} is still ${state} after ${waited}s; continuing" >&2
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  sudo systemctl reset-failed "$unit" >/dev/null 2>&1 || true
}

# Login sessions (including anyone SSH-ing in to diagnose a run) live under user.slice, so pinning
# that slice to the driver's cores means a diagnostic shell can never steal engine CPU mid-pass.
confine_login_sessions() {
  if sudo systemctl set-property --runtime user.slice AllowedCPUs="$DRIVER_CPUSET"; then
    echo "login sessions confined to driver cores ${DRIVER_CPUSET}; SSH diagnosis is safe during passes"
  else
    echo "could not confine user.slice; avoid running commands over SSH during measured passes" >&2
  fi
}

release_login_sessions() {
  sudo systemctl set-property --runtime user.slice AllowedCPUs="" >/dev/null 2>&1 || true
}

# shellcheck disable=SC2329
cleanup_units() {
  unit_stop bench-driver.service
  unit_stop bench-sirannon.service
  unit_stop bench-postgres.service
  release_login_sessions
}
trap cleanup_units EXIT
# A hard-killed prior run never reached its EXIT trap, so its units may still hold ports and cores.
cleanup_units
confine_login_sessions

run_with_deadline() {
  local secs="$1"
  shift
  local began rc=0 elapsed
  began="$(now_ms)"
  timeout -k 30 "$secs" "$@"
  rc=$?
  elapsed=$(( ($(now_ms) - began) / 1000 ))
  # 137 is timeout's SIGKILL or an OOM kill; only the elapsed check tells a crash from a deadline.
  if [ "$rc" -eq 124 ] || { [ "$rc" -eq 137 ] && [ "$elapsed" -ge "$secs" ]; }; then
    echo "pass exceeded its ${secs}s deadline; killed it and stopping the driver unit" >&2
    unit_stop bench-driver.service
    return 124
  fi
  if [ "$rc" -ne 0 ]; then
    echo "pass failed with exit ${rc} after ${elapsed}s" >&2
  fi
  return "$rc"
}

# shellcheck disable=SC2329
pg_probe() { "$PG_BINDIR/pg_isready" -q -h 127.0.0.1 -p 5432 -U benchmark; }
# shellcheck disable=SC2329
sirannon_probe() { "$NODE_BIN" -e "fetch('http://127.0.0.1:9876/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"; }

wait_probe() {
  local start_ms="$1" deadline
  shift
  deadline=$(( $(now_ms) + COLD_START_TIMEOUT * 1000 ))
  while [ "$(now_ms)" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then
      echo $(( $(now_ms) - start_ms ))
      return 0
    fi
    sleep 0.05
  done
  return 1
}

backing_disk() {
  local src disk
  src="$(findmnt -no SOURCE -T "$1" 2>/dev/null)" || return 1
  [ -n "$src" ] || return 1
  disk="$(lsblk -no PKNAME "$src" 2>/dev/null | head -n1)"
  [ -n "$disk" ] || disk="$(basename "$src")"
  printf '%s\n' "$disk"
}

check_data_device() {
  local dir="$1" disk rootdisk
  disk="$(backing_disk "$dir")" || { echo "cannot resolve the device behind ${dir}" >&2; return 1; }
  rootdisk="$(backing_disk /)" || rootdisk=""
  echo "data dir ${dir} is on /dev/${disk} (root disk: /dev/${rootdisk:-unknown})"
  case "$SSD_MODE" in
    required)
      if [ "$disk" = "$rootdisk" ] || [[ "$disk" != nvme* ]]; then
        echo "BENCH_LOCAL_SSD_MODE=required: ${dir} must be on a local NVMe disk separate from the root disk; refusing to seed" >&2
        return 1
      fi
      ;;
    root)
      if [ "$disk" != "$rootdisk" ]; then
        echo "BENCH_LOCAL_SSD_MODE=root: expected ${dir} on the NVMe root disk, found /dev/${disk}" >&2
        return 1
      fi
      ;;
  esac
  return 0
}

verify_engine_cgroup() {
  local unit="$1" cg="/sys/fs/cgroup/system.slice/$1"
  local page expect_mem actual_mem actual_cpus
  # The kernel rounds memory.max down to a page multiple, so the expectation is rounded to match.
  page="$(getconf PAGE_SIZE)"
  expect_mem="$(numfmt --from=iec "$ENGINE_MEMORY")"
  expect_mem=$(( expect_mem / page * page ))
  actual_mem="$(cat "$cg/memory.max" 2>/dev/null || echo missing)"
  actual_cpus="$(cat "$cg/cpuset.cpus.effective" 2>/dev/null || echo missing)"
  if [ "$actual_mem" != "$expect_mem" ] || ! same_cpuset "$actual_cpus" "$ENGINE_CPUSET"; then
    echo "${unit} is not under the configured caps (cpus: ${actual_cpus}, expected ${ENGINE_CPUSET}; memory.max: ${actual_mem}, expected ${expect_mem}); refusing to measure" >&2
    return 1
  fi
  echo "${unit} caps verified from the cgroup: cpus ${actual_cpus}, memory.max ${actual_mem}"
}

drop_caches() {
  sync
  printf '3\n' | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

cooldown() {
  echo "cooldown: sync, settle dirty pages (<= ${COOLDOWN_DIRTY_KB} kB), trim, pause ${COOLDOWN_SECONDS}s"
  sync
  local waited=0 dirty
  while :; do
    dirty="$(awk '/^(Dirty|Writeback):/ { total += $2 } END { print total + 0 }' /proc/meminfo)"
    [ "$dirty" -le "$COOLDOWN_DIRTY_KB" ] && break
    if [ "$waited" -ge "$COOLDOWN_TIMEOUT" ]; then
      echo "dirty pages still at ${dirty} kB after ${COOLDOWN_TIMEOUT}s; continuing" >&2
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  sudo fstrim "$(findmnt -no TARGET -T "$BENCH_DATA_ROOT")" >/dev/null 2>&1 || true
  sleep "$COOLDOWN_SECONDS"
}

# OOMScoreAdjust and the two PG_OOM_ADJUST variables are one mechanism: they exempt the postmaster
# while each backend resets to a normal score, so memory pressure kills a recoverable backend.
start_postgres() {
  sudo systemd-run --quiet --collect --unit=bench-postgres \
    -p AllowedCPUs="$ENGINE_CPUSET" -p MemoryMax="$ENGINE_MEMORY" -p MemorySwapMax=0 \
    -p User=postgres -p KillSignal=SIGINT -p OOMScoreAdjust=-1000 \
    --setenv=PG_OOM_ADJUST_FILE=/proc/self/oom_score_adj --setenv=PG_OOM_ADJUST_VALUE=0 \
    -p StandardOutput=append:"$RUN_DIR/postgres-server.log" \
    -p StandardError=append:"$RUN_DIR/postgres-server.log" \
    "$PG_BINDIR/postgres" -D "$PG_DATA_DIR" \
    -c listen_addresses=127.0.0.1 -c port=5432 -c unix_socket_directories=/tmp \
    -c shared_buffers=512MB -c work_mem=64MB -c effective_cache_size=1536MB \
    -c maintenance_work_mem=256MB -c wal_level=minimal -c max_wal_senders=0 \
    -c checkpoint_timeout=15min -c max_wal_size=1GB -c random_page_cost=1.1
}

start_sirannon() {
  local durability="$1"
  rm -rf "$SIRANNON_DATA_DIR"
  mkdir -p "$SIRANNON_DATA_DIR"
  sudo systemd-run --quiet --collect --unit=bench-sirannon \
    -p AllowedCPUs="$ENGINE_CPUSET" -p MemoryMax="$ENGINE_MEMORY" -p MemorySwapMax=0 \
    -p User="$RUN_AS_USER" -p WorkingDirectory="$REPO_ROOT" \
    -p StandardOutput=append:"$RUN_DIR/sirannon-server.log" \
    -p StandardError=append:"$RUN_DIR/sirannon-server.log" \
    --setenv=HOST=127.0.0.1 --setenv=PORT=9876 --setenv=BENCH_SIRANNON_DB=bench \
    --setenv=BENCH_DURABILITY="$durability" --setenv=BENCH_DATA_DIR="$SIRANNON_DATA_DIR" \
    "$NODE_BIN" benchmarks/server/sirannon-server.mjs
}

driver_env_args() {
  DRIVER_ENV_ARGS=()
  local var
  for var in BENCH_SIRANNON_URL BENCH_SIRANNON_DB BENCH_PG_HOST BENCH_PG_PORT BENCH_PG_USER \
    BENCH_PG_PASSWORD BENCH_PG_DATABASE BENCH_PG_POOL_SIZE BENCH_RUN_ID BENCH_RESULTS_DIR \
    BENCH_MACHINE_LABEL BENCH_DATA_SIZE BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_RUNS \
    BENCH_SEED BENCH_WORKLOADS BENCH_TARGET_RATES BENCH_SCALING_WORKLOADS BENCH_SLO_P99_MS \
    BENCH_SOAK_SECONDS BENCH_SOAK_WORKLOADS \
    BENCH_MAX_IN_FLIGHT BENCH_DRIVER_CPUS BENCH_ENGINE_CPUS BENCH_DRIVER_CPUSET \
    BENCH_ENGINE_CPUSET BENCH_ENGINE_MEMORY BENCH_CDC_SAMPLES BENCH_CDC_WARMUP; do
    if [ -n "${!var:-}" ]; then
      DRIVER_ENV_ARGS+=("--setenv=${var}=${!var}")
    fi
  done
}

run_driver_pass() {
  driver_env_args
  run_with_deadline "$BENCH_PASS_TIMEOUT" \
    sudo systemd-run --quiet --wait --collect --pipe --unit=bench-driver \
    -p AllowedCPUs="$DRIVER_CPUSET" -p User="$RUN_AS_USER" -p WorkingDirectory="$DRIVER_DIR" \
    "${DRIVER_ENV_ARGS[@]}" \
    "$NODE_BIN" src/cli.ts "$@"
}

init_postgres() {
  local pwfile="$BENCH_DATA_ROOT/.pg-init-password"
  printf '%s\n' "$BENCH_PG_PASSWORD" | sudo install -m 600 -o postgres -g postgres /dev/stdin "$pwfile"
  local rc=0
  sudo -u postgres "$PG_BINDIR/initdb" -D "$PG_DATA_DIR" -U benchmark -A scram-sha-256 \
    --pwfile="$pwfile" >/dev/null || rc=1
  sudo rm -f "$pwfile"
  return "$rc"
}

record_engine_caps_proof() {
  local unit="$1" engine="$2" durability="$3" cg="/sys/fs/cgroup/system.slice/$1"
  printf '%s durability=%s cpuset=%s memory.max=%s memory.peak=%s\n' \
    "$engine" "$durability" \
    "$(cat "$cg/cpuset.cpus.effective" 2>/dev/null || echo unknown)" \
    "$(cat "$cg/memory.max" 2>/dev/null || echo unknown)" \
    "$(cat "$cg/memory.peak" 2>/dev/null || echo unknown)" \
    >> "$RUN_DIR/resource-control.log"
}

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
  sirannon_args=(--engine sirannon --durability "$durability")
  if [ "$index" -eq 0 ]; then
    sirannon_args+=(--features)
  fi
  if start_sirannon "$durability" \
    && wait_probe "$(now_ms)" sirannon_probe >/dev/null \
    && verify_engine_cgroup bench-sirannon.service; then
    drop_caches
    run_driver_pass "${sirannon_args[@]}" || status=1
    record_engine_caps_proof bench-sirannon.service sirannon "$durability"
  else
    echo "sirannon is not healthy under the verified caps; skipping its ${durability} pass" >&2
    status=1
  fi
  unit_stop bench-sirannon.service
  rm -rf "$SIRANNON_DATA_DIR"
  cooldown

  if start_postgres \
    && wait_probe "$(now_ms)" pg_probe >/dev/null \
    && verify_engine_cgroup bench-postgres.service; then
    drop_caches
    run_driver_pass --engine postgres --durability "$durability" || status=1
    record_engine_caps_proof bench-postgres.service postgres "$durability"
  else
    echo "postgres is not healthy under the verified caps; skipping its ${durability} pass" >&2
    status=1
  fi
  unit_stop bench-postgres.service
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
