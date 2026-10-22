#!/usr/bin/env bash

# shellcheck disable=SC2329
pg_probe() { "$PG_BINDIR/pg_isready" -q -h 127.0.0.1 -p 5432 -U benchmark; }
# shellcheck disable=SC2329
sirannon_probe() { "$NODE_BIN" -e "fetch(process.argv[1]).then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))" "http://${ENGINE_ADDRESS}:9876/health"; }

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

init_postgres() {
  local pwfile="$BENCH_DATA_ROOT/.pg-init-password"
  printf '%s\n' "$BENCH_PG_PASSWORD" | sudo install -m 600 -o postgres -g postgres /dev/stdin "$pwfile"
  local rc=0
  sudo -u postgres "$PG_BINDIR/initdb" -D "$PG_DATA_DIR" -U benchmark -A scram-sha-256 \
    --pwfile="$pwfile" >/dev/null || rc=1
  sudo rm -f "$pwfile"
  if [ "$rc" -eq 0 ] && load_remote; then
    printf 'host all benchmark %s/32 scram-sha-256\n' "$LOAD_ADDRESS" \
      | sudo -u postgres tee -a "$PG_DATA_DIR/pg_hba.conf" >/dev/null || rc=1
  fi
  return "$rc"
}

pg_listen_addresses() {
  if [ "$ENGINE_ADDRESS" = "127.0.0.1" ]; then
    printf '127.0.0.1'
  else
    printf '127.0.0.1,%s' "$ENGINE_ADDRESS"
  fi
}

start_postgres() {
  sudo systemd-run --quiet --collect --unit=bench-postgres \
    -p AllowedCPUs="$ENGINE_CPUSET" -p MemoryMax="$ENGINE_MEMORY" -p MemorySwapMax=0 \
    -p User=postgres -p KillSignal=SIGINT -p OOMScoreAdjust=-1000 \
    --setenv=PG_OOM_ADJUST_FILE=/proc/self/oom_score_adj --setenv=PG_OOM_ADJUST_VALUE=0 \
    -p StandardOutput=append:"$RUN_DIR/postgres-server.log" \
    -p StandardError=append:"$RUN_DIR/postgres-server.log" \
    "$PG_BINDIR/postgres" -D "$PG_DATA_DIR" \
    -c listen_addresses="$(pg_listen_addresses)" -c port=5432 -c unix_socket_directories=/tmp \
    -c max_connections="$PG_MAX_CONNECTIONS" \
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
    --setenv=HOST="$ENGINE_ADDRESS" --setenv=PORT=9876 --setenv=BENCH_SIRANNON_DB=bench \
    --setenv=BENCH_DURABILITY="$durability" --setenv=BENCH_DATA_DIR="$SIRANNON_DATA_DIR" \
    --setenv=BENCH_WRITE_TIMEOUT_MS="$BENCH_WRITE_TIMEOUT_MS" \
    "$NODE_BIN" benchmarks/server/sirannon-server.mjs
}

driver_env_args() {
  DRIVER_ENV_ARGS=()
  local var value
  for var in BENCH_SIRANNON_URL BENCH_SIRANNON_DB BENCH_PG_HOST BENCH_PG_PORT BENCH_PG_USER \
    BENCH_PG_PASSWORD BENCH_PG_DATABASE BENCH_RUN_ID BENCH_RESULTS_DIR \
    BENCH_MACHINE_LABEL BENCH_DATA_SIZE BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_RUNS \
    BENCH_SEED BENCH_WORKLOADS BENCH_TARGET_RATES BENCH_SCALING_WORKLOADS BENCH_SLO_P99_MS \
    BENCH_SWEEP_STOP_STEPS BENCH_SOAK_SECONDS BENCH_SOAK_WORKLOADS BENCH_PREPARE_RETRY_SECONDS \
    BENCH_REQUEST_TIMEOUT_MS BENCH_WORKLOAD_TIMEOUT_MS BENCH_WRITE_TIMEOUT_MS BENCH_SCHEMA_TIMEOUT_MS \
    BENCH_CONCURRENCY_LEVELS BENCH_DRIVER_CPUS BENCH_ENGINE_CPUS BENCH_ENGINE_CGROUP BENCH_ENGINE_CPU_URL \
    BENCH_NETWORK_PROBE BENCH_DRIVER_CPUSET BENCH_ENGINE_CPUSET BENCH_ENGINE_MEMORY BENCH_CDC_SAMPLES \
    BENCH_CDC_WARMUP; do
    value="${!var:-}"
    if load_remote; then
      case "$var" in
        BENCH_MACHINE_LABEL) value="${BENCH_LOAD_MACHINE_LABEL:-}" ;;
        BENCH_RESULTS_DIR) value="$LOAD_RESULTS_DIR" ;;
        BENCH_DRIVER_CPUSET | BENCH_ENGINE_CGROUP) value="" ;;
        BENCH_ENGINE_CPU_URL)
          if [ -n "${BENCH_ENGINE_CGROUP:-}" ]; then
            value="http://${ENGINE_ADDRESS}:${CPU_STAT_PORT}/${BENCH_ENGINE_CGROUP##*/}"
          fi
          ;;
      esac
    fi
    if [ -n "$value" ]; then
      DRIVER_ENV_ARGS+=("--setenv=${var}=${value}")
    fi
  done
}

run_driver_pass() {
  if load_remote; then
    run_driver_pass_remote "$@"
    return
  fi
  driver_env_args
  run_with_deadline "$BENCH_PASS_TIMEOUT" \
    sudo systemd-run --quiet --wait --collect --pipe --unit=bench-driver \
    -p AllowedCPUs="$DRIVER_CPUSET" -p User="$RUN_AS_USER" -p WorkingDirectory="$DRIVER_DIR" \
    "${DRIVER_ENV_ARGS[@]}" \
    "$NODE_BIN" src/cli.ts "$@"
}
