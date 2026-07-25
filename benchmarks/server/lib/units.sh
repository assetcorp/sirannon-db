#!/usr/bin/env bash

now_ms() {
  local us="${EPOCHREALTIME//[!0-9]/}"
  printf '%s\n' $(( us / 1000 ))
}

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

run_with_deadline() {
  local secs="$1"
  shift
  local began rc=0 elapsed
  began="$(now_ms)"
  timeout -k 30 "$secs" "$@"
  rc=$?
  elapsed=$(( ($(now_ms) - began) / 1000 ))
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

record_engine_caps_proof() {
  local unit="$1" engine="$2" durability="$3" cg="/sys/fs/cgroup/system.slice/$1"
  printf '%s durability=%s cpuset=%s memory.max=%s memory.peak=%s\n' \
    "$engine" "$durability" \
    "$(cat "$cg/cpuset.cpus.effective" 2>/dev/null || echo unknown)" \
    "$(cat "$cg/memory.max" 2>/dev/null || echo unknown)" \
    "$(cat "$cg/memory.peak" 2>/dev/null || echo unknown)" \
    >> "$RUN_DIR/resource-control.log"
}
