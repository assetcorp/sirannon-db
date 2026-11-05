#!/usr/bin/env bash

LOAD_POLL_SECONDS=5
LOAD_UNREACHABLE_POLLS=60
CPU_STAT_PORT=9877

load_remote() { [ -n "${LOAD_ADDRESS:-}" ]; }

load_ssh() {
  ssh -i "$LOAD_SSH_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o LogLevel=ERROR \
    -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4 \
    "${LOAD_USER}@${LOAD_ADDRESS}" "$@" </dev/null
}

quoted() { printf '%q ' "$@"; }

prepare_load_host() {
  echo "building the SDK and installing driver dependencies on the load machine ${LOAD_ADDRESS}"
  load_ssh "export COREPACK_ENABLE_DOWNLOAD_PROMPT=0 && cd $(quoted "$LOAD_REPO") && pnpm install --frozen-lockfile && pnpm --filter @delali/sirannon-db build && cd benchmarks/server/driver && pnpm install --ignore-workspace --frozen-lockfile" || return 1
  LOAD_NODE_BIN="$(load_ssh 'command -v node')" || return 1
  LOAD_CPUS="$(load_ssh nproc)" || return 1
  LOAD_RESULTS_DIR="${LOAD_REPO}/benchmarks/server/${HOST_RESULTS_DIR}"
  LOAD_STATE_DIR="${LOAD_RESULTS_DIR}/.driver"
  LOAD_RUN_DIR="${LOAD_RESULTS_DIR}/runs/${BENCH_RUN_ID}"
  load_ssh "mkdir -p $(quoted "$LOAD_STATE_DIR")" || return 1
  [ -n "$LOAD_NODE_BIN" ] && [ -n "$LOAD_CPUS" ] || { echo "could not read node or nproc on the load machine" >&2; return 1; }
}

stop_load_driver() {
  load_remote || return 0
  load_ssh "sudo systemctl stop bench-driver.service >/dev/null 2>&1; sudo systemctl reset-failed bench-driver.service >/dev/null 2>&1; true" \
    || echo "could not reach the load machine to stop its driver unit" >&2
}

start_cpu_stat_server() {
  sudo systemd-run --quiet --collect --unit=bench-cpu-stat -p User="$RUN_AS_USER" \
    python3 "$(pwd)/lib/cpu-stat-server.py" "$ENGINE_ADDRESS" "$CPU_STAT_PORT" || return 1
  local attempt
  for attempt in $(seq 1 50); do
    if python3 -c "import socket,sys; socket.create_connection((sys.argv[1], int(sys.argv[2])), 1).close()" \
      "$ENGINE_ADDRESS" "$CPU_STAT_PORT" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "the engine CPU service never listened on ${ENGINE_ADDRESS}:${CPU_STAT_PORT} after ${attempt} tries" >&2
  return 1
}

load_poll_status() {
  local status_file="$1"
  load_ssh "cat $(quoted "$status_file") 2>/dev/null || { systemctl is-active --quiet bench-driver.service || cat $(quoted "$status_file") 2>/dev/null || echo lost; }"
}

collect_load_results() {
  mkdir -p "$RUN_DIR" || return 1
  load_ssh "test -d $(quoted "$LOAD_RUN_DIR") && tar -C $(quoted "$LOAD_RUN_DIR") -cf - ." | tar -C "$RUN_DIR" -xf -
}

run_driver_pass_remote() {
  driver_env_args
  local tag log status_file launch
  tag="$(date -u +%H%M%S)-$$"
  log="${LOAD_STATE_DIR}/driver-${tag}.log"
  status_file="${LOAD_STATE_DIR}/driver-${tag}.status"
  launch="$(quoted sudo systemd-run --quiet --collect --unit=bench-driver \
    -p User="$LOAD_USER" -p WorkingDirectory="${LOAD_REPO}/benchmarks/server/driver" \
    -p StandardOutput=append:"$log" -p StandardError=append:"$log" \
    "${DRIVER_ENV_ARGS[@]}" \
    /bin/bash "${LOAD_REPO}/benchmarks/server/lib/driver-pass.sh" "$status_file" "$LOAD_NODE_BIN" src/cli.ts "$@")"
  stop_load_driver
  load_ssh "$launch" || { echo "could not start the driver on the load machine" >&2; return 1; }

  local began chunk offset=0 rc="" misses=0 received
  began="$(now_ms)"
  chunk="$(mktemp)"
  while :; do
    if load_ssh "tail -c +$((offset + 1)) $(quoted "$log") 2>/dev/null" >"$chunk"; then
      received="$(wc -c <"$chunk" | tr -d ' ')"
      cat "$chunk"
      offset=$((offset + received))
    fi
    if rc="$(load_poll_status "$status_file")"; then
      misses=0
      rc="$(printf '%s' "$rc" | tr -d '[:space:]')"
      [ -n "$rc" ] && break
    else
      rc=""
      misses=$((misses + 1))
      if [ "$misses" -ge "$LOAD_UNREACHABLE_POLLS" ]; then
        echo "the load machine stayed unreachable for ${misses} polls; treating the pass as failed" >&2
        rc=1
        break
      fi
    fi
    if [ $(( ($(now_ms) - began) / 1000 )) -ge "$BENCH_PASS_TIMEOUT" ]; then
      echo "pass exceeded its ${BENCH_PASS_TIMEOUT}s deadline; stopping the driver on the load machine" >&2
      stop_load_driver
      rc=124
      break
    fi
    sleep "$LOAD_POLL_SECONDS"
  done
  load_ssh "tail -c +$((offset + 1)) $(quoted "$log") 2>/dev/null" || true
  rm -f "$chunk"
  if [ "$rc" = "lost" ]; then
    echo "the driver unit on the load machine ended without recording an exit status" >&2
    rc=1
  fi
  collect_load_results || { echo "could not copy the pass results back from the load machine" >&2; rc=1; }
  case "$rc" in
    '' | *[!0-9]*) rc=1 ;;
  esac
  if [ "$rc" -ne 0 ]; then
    echo "pass failed with exit ${rc}" >&2
  fi
  return "$rc"
}

write_engine_machine() {
  python3 - "$RUN_DIR/engine-machine.json" "${BENCH_MACHINE_LABEL:-}" "$ENGINE_CPUSET" "$ENGINE_MEMORY" <<'PY'
import json
import os
import platform
import sys

path, label, cpuset, memory = sys.argv[1:5]


def cpu_model() -> str | None:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def total_memory_bytes() -> int | None:
    try:
        with open("/proc/meminfo", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def os_name() -> str:
    try:
        with open("/etc/os-release", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("PRETTY_NAME="):
                    return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return platform.system()


payload = {
    "machine_label": label or None,
    "cpu_model": cpu_model(),
    "logical_cpus": os.cpu_count(),
    "total_memory_bytes": total_memory_bytes(),
    "os": os_name(),
    "kernel": platform.release(),
    "arch": platform.machine(),
    "engine_allowed_cpus": cpuset,
    "engine_memory_max": memory,
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")
PY
}
