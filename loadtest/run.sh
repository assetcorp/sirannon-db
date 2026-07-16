#!/usr/bin/env bash
# Each rate list ends by repeating an earlier healthy rate; comparing the two is the recovery check.
# The driver runs on the host, outside the container's allowance, so only the server's share varies.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DRIVER_DIR="$REPO_ROOT/benchmarks/server/driver"
IMAGE="${LOADTEST_IMAGE:-sirannon-loadtest:local}"
CONTAINER="${LOADTEST_CONTAINER:-sirannon-loadtest}"
PORT="${LOADTEST_PORT:-19876}"
RESULTS_DIR="${LOADTEST_RESULTS_DIR:-$REPO_ROOT/loadtest/results}"

CELLS="${LOADTEST_CELLS:-1:1g 1:4g 2:1g 2:4g}"
DURABILITIES="${LOADTEST_DURABILITIES:-matched full}"
WORKLOADS="${LOADTEST_WORKLOADS:-ycsb-c batch-update tpc-c-new-order ycsb-b}"
DATA_SIZE="${LOADTEST_DATA_SIZE:-1500000}"
RUNS="${LOADTEST_RUNS:-3}"
WARMUP="${LOADTEST_WARMUP_SECONDS:-2}"
MEASURE="${LOADTEST_MEASURE_SECONDS:-10}"
MAX_IN_FLIGHT="${LOADTEST_MAX_IN_FLIGHT:-2048}"
START_TIMEOUT="${LOADTEST_START_TIMEOUT:-90}"

rates_for() {
  case "$1" in
    tpc-c-new-order) echo "${LOADTEST_RATES_TXN:-500,2000,8000,20000,50000,100000,2000}" ;;
    batch-update) echo "${LOADTEST_RATES_WRITE:-1000,5000,20000,50000,100000,200000,5000}" ;;
    *) echo "${LOADTEST_RATES_READ:-1000,5000,20000,50000,100000,200000,5000}" ;;
  esac
}

log() { printf '%s %s\n' "[$(date -u +%H:%M:%S)]" "$*"; }

stop_container() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }

start_container() {
  local cpus="$1" memory="$2" durability="$3"
  stop_container
  # A health probe alone would pass against any server already holding the port.
  if ! docker run -d --name "$CONTAINER" \
    --cpus="$cpus" -m "$memory" --memory-swap="$memory" \
    -p "$PORT:9876" \
    -e "BENCH_DURABILITY=$durability" \
    "$IMAGE" >/dev/null; then
    log "docker run failed for cpu=$cpus mem=$memory (is port $PORT free?)"
    return 1
  fi

  local deadline=$((SECONDS + START_TIMEOUT))
  while [ "$SECONDS" -lt "$deadline" ]; do
    if curl -fsS -m 2 "http://127.0.0.1:$PORT/health" >/dev/null 2>&1; then
      return 0
    fi
    if [ -z "$(docker ps -q -f name="^${CONTAINER}$")" ]; then
      log "container exited before serving; last output:"
      docker logs "$CONTAINER" 2>&1 | tail -5
      return 1
    fi
    sleep 1
  done
  log "container did not become healthy within ${START_TIMEOUT}s"
  return 1
}

# Read back from the live cgroup, so a cell can never report a limit it did not run under.
verify_caps() {
  local cpus="$1" memory="$2"
  local cpu_max memory_max expected_quota expected_bytes
  cpu_max="$(docker exec "$CONTAINER" cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo missing)"
  memory_max="$(docker exec "$CONTAINER" cat /sys/fs/cgroup/memory.max 2>/dev/null || echo missing)"
  expected_quota="$(awk -v c="$cpus" 'BEGIN { printf "%d", c * 100000 }')"
  expected_bytes="$(awk -v m="$memory" 'BEGIN {
    unit = tolower(substr(m, length(m)))
    n = substr(m, 1, length(m) - 1)
    if (unit == "g") printf "%d", n * 1024 * 1024 * 1024
    else if (unit == "m") printf "%d", n * 1024 * 1024
    else printf "%s", m
  }')"
  if [ "${cpu_max% *}" != "$expected_quota" ]; then
    log "cpu.max is ${cpu_max}, expected quota ${expected_quota}; refusing to measure this cell"
    return 1
  fi
  if [ "$memory_max" != "$expected_bytes" ]; then
    log "memory.max is ${memory_max}, expected ${expected_bytes}; refusing to measure this cell"
    return 1
  fi
  printf '%s\n' "$memory_max" >"$CELL_DIR/memory.max"
  printf '%s\n' "$cpu_max" >"$CELL_DIR/cpu.max"
  log "caps verified from the cgroup: cpu.max=${cpu_max} memory.max=${memory_max}"
}

run_cell() {
  local cpus="$1" memory="$2" durability="$3" workload="$4"
  local run_id="cpu${cpus}-mem${memory}-${durability}-${workload}"
  CELL_DIR="$RESULTS_DIR/$run_id"
  mkdir -p "$CELL_DIR"

  if ! start_container "$cpus" "$memory" "$durability"; then
    printf 'container-failed\n' >"$CELL_DIR/status"
    docker logs "$CONTAINER" >"$CELL_DIR/server.log" 2>&1 || true
    return 0
  fi
  if ! verify_caps "$cpus" "$memory"; then
    printf 'caps-mismatch\n' >"$CELL_DIR/status"
    return 0
  fi

  log "cpu=$cpus mem=$memory durability=$durability workload=$workload rates=$(rates_for "$workload")"
  set +e
  ( cd "$DRIVER_DIR" && \
    BENCH_SIRANNON_URL="http://127.0.0.1:$PORT" \
    BENCH_SIRANNON_DB=bench \
    BENCH_WORKLOADS="$workload" \
    BENCH_TARGET_RATES="$(rates_for "$workload")" \
    BENCH_DATA_SIZE="$DATA_SIZE" \
    BENCH_RUNS="$RUNS" \
    BENCH_WARMUP_SECONDS="$WARMUP" \
    BENCH_MEASURE_SECONDS="$MEASURE" \
    BENCH_MAX_IN_FLIGHT="$MAX_IN_FLIGHT" \
    BENCH_RESULTS_DIR="$CELL_DIR" \
    BENCH_RUN_ID=cell \
    node src/cli.ts --engine sirannon --durability "$durability" ) \
    >"$CELL_DIR/driver.log" 2>&1
  local rc=$?
  set -e

  docker logs "$CONTAINER" >"$CELL_DIR/server.log" 2>&1 || true
  if [ "$rc" -ne 0 ]; then
    printf 'driver-failed rc=%s\n' "$rc" >"$CELL_DIR/status"
    log "driver exited $rc; kept the logs and carried on"
  elif [ -z "$(docker ps -q -f name="^${CONTAINER}$")" ]; then
    printf 'server-died\n' >"$CELL_DIR/status"
    log "server is gone after the pass"
  else
    printf 'ok\n' >"$CELL_DIR/status"
  fi
  stop_container
}

main() {
  command -v docker >/dev/null || { echo "docker is required" >&2; exit 2; }
  docker image inspect "$IMAGE" >/dev/null 2>&1 || {
    log "building $IMAGE"
    docker build -f "$REPO_ROOT/loadtest/Dockerfile" -t "$IMAGE" "$REPO_ROOT"
  }
  mkdir -p "$RESULTS_DIR"
  trap stop_container EXIT

  for cell in $CELLS; do
    for durability in $DURABILITIES; do
      for workload in $WORKLOADS; do
        run_cell "${cell%%:*}" "${cell##*:}" "$durability" "$workload"
      done
    done
  done
  log "results in $RESULTS_DIR"
}

main "$@"
