#!/usr/bin/env bash

CLOUD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$CLOUD_DIR/../.." && pwd)"

PROVIDER="${PROVIDER:-gcp}"
VM_NAME="${VM_NAME:-sirannon-bench}"
DISK_SIZE="${DISK_SIZE:-60}"
DRY_RUN="${DRY_RUN:-0}"
MACHINE_LABEL="${BENCH_MACHINE_LABEL:-}"
MACHINE_TYPE="${MACHINE_TYPE:-}"
VM_MAX_HOURS="${VM_MAX_HOURS:-24}"

CLOUD_PROFILE_WORST_CASE_RUN_HOURS=22
SMOKE_PROFILE_WORST_CASE_RUN_HOURS=2

RESULTS_REL="benchmarks/server/results"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

case "$VM_MAX_HOURS" in
  '' | *[!0-9]*) die "VM_MAX_HOURS must be a whole number of hours (0 disables the self-delete backstop)" ;;
esac

_run() {
  if [ "$DRY_RUN" = "1" ]; then
    printf 'DRY-RUN: %s\n' "$*"
    return 0
  fi
  "$@"
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 && return 0
  [ "$DRY_RUN" = "1" ] && { log "(dry-run) '$1' is not installed; commands will only be printed"; return 0; }
  die "$1 is required but not found"
}

prov_wait_ssh() {
  [ "$DRY_RUN" = "1" ] && { log "(dry-run) would wait for SSH"; return 0; }
  log "wait for SSH"
  local attempt
  for ((attempt = 1; attempt <= 40; attempt++)); do
    if prov_ssh true >/dev/null 2>&1; then
      log "SSH ready after ${attempt} attempt(s)"
      return 0
    fi
    sleep 5
  done
  die "VM never became reachable over SSH"
}

cmd_up() {
  if [ "$DRY_RUN" != "1" ] && prov_exists && on_load_vm prov_exists; then
    log "$VM_NAME and $LOAD_VM_NAME already exist"
    prov_lifetime_note
    on_load_vm prov_lifetime_note
    return 0
  fi
  log "create the engine VM $VM_NAME ($MACHINE_TYPE) and the load VM $LOAD_VM_NAME ($LOAD_MACHINE_TYPE)"
  if [ "$DRY_RUN" != "1" ] && [ "${ASSUME_YES:-0}" != "1" ]; then
    read -r -p "This starts billing for both VMs until you run 'down'. Proceed? [y/N] " reply
    [ "$reply" = "y" ] || [ "$reply" = "Y" ] || die "aborted"
  fi
  prov_network_up
  vm_create_if_missing
  on_load_vm vm_create_if_missing
  prov_lifetime_note
  on_load_vm prov_lifetime_note
  prov_wait_ssh
  on_load_vm prov_wait_ssh
}

vm_lifetime_guard() {
  [ "$DRY_RUN" = "1" ] && return 0
  [ "$VM_MAX_HOURS" = "0" ] && return 0
  declare -F prov_termination_epoch >/dev/null || return 0
  local deadline now left_hours required
  deadline="$(prov_termination_epoch)" || deadline=""
  if [ -z "$deadline" ]; then
    log "could not confirm a self-delete deadline on $VM_NAME; if it predates the backstop, recreate it (down, then up) to arm one"
    return 0
  fi
  case "${BENCH_PROFILE:-cloud}" in
    smoke) required="$SMOKE_PROFILE_WORST_CASE_RUN_HOURS" ;;
    *) required="$CLOUD_PROFILE_WORST_CASE_RUN_HOURS" ;;
  esac
  now="$(date +%s)"
  left_hours=$(( (deadline - now) / 3600 ))
  [ "$left_hours" -lt 0 ] && left_hours=0
  if [ $(( deadline - now )) -lt $(( required * 3600 )) ]; then
    die "the VM self-deletes in about ${left_hours}h, but a '${BENCH_PROFILE:-cloud}' run can need up to ${required}h; recreate the VM (down, then up) or raise VM_MAX_HOURS"
  fi
  log "self-delete deadline is about ${left_hours}h away; a '${BENCH_PROFILE:-cloud}' run needs at most ${required}h"
}

sync_unpack() {
  log "upload and unpack to ~/sirannon on $VM_NAME"
  prov_scp_up "$1" "sirannon.tgz"
  prov_ssh "rm -rf sirannon.next && mkdir -p sirannon.next && tar xzf sirannon.tgz -C sirannon.next && rm -f sirannon.tgz && bash sirannon.next/benchmarks/cloud/remote-swap-checkout.sh"
}

cmd_sync() {
  command -v git >/dev/null 2>&1 || die "git is required to package the working tree"
  log "package the working tree via git (honours .gitignore, keeps .git for the build stamp)"
  if [ "$DRY_RUN" = "1" ]; then
    sync_unpack "<working-tree>.tgz"
    on_load_vm sync_unpack "<working-tree>.tgz"
    return 0
  fi
  local tarball filelist
  tarball="$(mktemp "${TMPDIR:-/tmp}/sirannon-sync.XXXXXX")"
  filelist="$(mktemp "${TMPDIR:-/tmp}/sirannon-files.XXXXXX")"
  {
    git -C "$REPO_ROOT" ls-files -z
    git -C "$REPO_ROOT" ls-files --others --exclude-standard -z -- .
    printf '.git\0'
  } >"$filelist"
  COPYFILE_DISABLE=1 tar czf "$tarball" -C "$REPO_ROOT" --null -T "$filelist"
  rm -f "$filelist"
  local rc=0
  sync_unpack "$tarball" || rc=1
  on_load_vm sync_unpack "$tarball" || rc=1
  rm -f "$tarball"
  [ "$rc" -eq 0 ] || die "could not push the working tree to both VMs"
}

cmd_setup() {
  log "prepare local NVMe and install PostgreSQL 18, Node 24, pnpm, and Python 3 on the engine VM $VM_NAME"
  prov_ssh "BENCH_LOCAL_SSD_MODE='${LOCAL_SSD_MODE:-auto}' bash sirannon/benchmarks/cloud/remote-bootstrap.sh" \
    || die "the engine VM setup failed"
  log "install Node 24, pnpm, and Python 3 on the load VM $LOAD_VM_NAME"
  on_load_vm prov_ssh "BENCH_VM_ROLE=load bash sirannon/benchmarks/cloud/remote-bootstrap.sh" \
    || die "the load VM setup failed"
  link_load_vm
}

cmd_run() {
  vm_lifetime_guard
  on_load_vm vm_lifetime_guard
  log "launch the benchmark run (detached; survives an SSH drop)"
  local forward
  forward="$(printf '%q ' "BENCH_MACHINE_LABEL=${MACHINE_LABEL}")"
  forward+="$(load_vm_forward)" || exit 1
  forward+="$(printf '%q ' "BENCH_LOCAL_SSD_MODE=${LOCAL_SSD_MODE:-auto}")"
  local v
  for v in BENCH_PROFILE BENCH_DURABILITIES BENCH_ENGINES BENCH_RUN_ID BENCH_DATA_SIZE BENCH_WORKLOADS \
    BENCH_TARGET_RATES BENCH_SCALING_WORKLOADS BENCH_RUNS BENCH_SEED \
    BENCH_SWEEP_STOP_STEPS BENCH_SOAK_SECONDS BENCH_SOAK_WORKLOADS BENCH_PREPARE_RETRY_SECONDS \
    BENCH_REQUEST_TIMEOUT_MS BENCH_WORKLOAD_TIMEOUT_MS BENCH_WRITE_TIMEOUT_MS BENCH_SCHEMA_TIMEOUT_MS \
    BENCH_WARMUP_SECONDS BENCH_MEASURE_SECONDS BENCH_SLO_P99_MS BENCH_CONCURRENCY_LEVELS \
    BENCH_ENGINE_CPUSET BENCH_ENGINE_MEMORY BENCH_CDC_SAMPLES \
    BENCH_CDC_WARMUP BENCH_DATA_ROOT BENCH_COOLDOWN_SECONDS BENCH_COOLDOWN_DIRTY_KB \
    BENCH_COOLDOWN_TIMEOUT BENCH_COLD_START_TIMEOUT BENCH_PASS_TIMEOUT; do
    if [ -n "${!v:-}" ]; then
      forward+="$(printf '%q ' "$v=${!v}")"
    fi
  done
  prov_ssh "bash sirannon/benchmarks/cloud/remote-launch.sh $forward"
  cmd_logs
}

cmd_logs() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would stream ~/bench.log until the run finishes"
    return 0
  fi
  log "stream ~/bench.log until the run finishes (Ctrl-C detaches, run keeps going)"

  local mirror detached=0 misses=0 shown next st
  mirror="$(mktemp "${TMPDIR:-/tmp}/sirannon-bench-log.XXXXXX")"
  trap 'detached=1' INT
  while :; do
    shown="$(wc -l <"$mirror" | tr -d ' ')"
    next=$((shown + 1))
    if [ "$shown" -gt 0 ]; then
      log "stream reconnecting (resuming from line $next)"
    fi
    # shellcheck disable=SC2016
    prov_ssh 'tail -n +'"$next"' --follow=name --pid=$(cat bench.pid 2>/dev/null || echo 1) bench.log' \
      < /dev/null | tee -a "$mirror" || true
    if [ "$detached" = "1" ]; then
      break
    fi
    st="$(prov_ssh 'cat bench.status 2>/dev/null || echo running' </dev/null | tr -d '\r')" || st="unreachable"
    case "$st" in
      running | unreachable | "") ;;
      *) break ;;
    esac
    if [ "$(wc -l <"$mirror" | tr -d ' ')" -eq "$shown" ]; then
      misses=$((misses + 1))
    else
      misses=0
    fi
    if [ "$misses" -ge 20 ]; then
      log "no progress after $misses reconnect attempts; giving up on the stream (the run may still be going)"
      break
    fi
    sleep 5
  done
  trap - INT
  rm -f "$mirror"

  if [ "$detached" = "1" ]; then
    log "still running (you detached); re-attach with: PROVIDER=$PROVIDER run-cloud.sh logs"
    return 0
  fi
  st="$(prov_ssh 'cat bench.status 2>/dev/null || echo running' </dev/null | tr -d '\r')" || st="running"
  case "$st" in
    0) log "run finished cleanly" ;;
    running) log "stream ended but the run may still be going; re-attach with: PROVIDER=$PROVIDER run-cloud.sh logs" ;;
    *) log "run reported failures (status $st); inspect with: PROVIDER=$PROVIDER run-cloud.sh ssh" ;;
  esac
}

cmd_fetch() {
  local remote="$RESULTS_REL" dest="$REPO_ROOT/$RESULTS_REL"
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would fetch new run directories from $remote/runs"
    return 0
  fi

  local fetched="" dir runs run
  for dir in runs .smoke/runs; do
    runs="$(prov_ssh "find sirannon/$remote/$dir -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null || true" | tr -d '\r')"
    [ -n "$runs" ] || continue
    fetched=1
    mkdir -p "$dest/$dir"
    for run in $runs; do
      if [ ! -e "$dest/$dir/$run" ]; then
        log "fetch run $run"
        prov_scp_down "sirannon/$remote/$dir/$run" "$dest/$dir/"
        continue
      fi
      log "refresh run $run (a pass may have been added or replaced)"
      local incoming="$dest/$dir/.incoming-$run"
      rm -rf "$incoming"
      if prov_scp_down "sirannon/$remote/$dir/$run" "$incoming"; then
        rm -rf "${dest:?}/${dir:?}/${run:?}"
        mv "$incoming" "$dest/$dir/$run"
      else
        rm -rf "$incoming"
        log "could not refresh run $run; the copy already here is untouched"
      fi
    done
  done
  if [ -z "$fetched" ]; then
    log "no run directories under $remote yet"
  fi

  log "results copied under $RESULTS_REL/ in this repo (not committed; commit the ones you publish)"
}

cmd_down() {
  local rc=0
  on_load_vm vm_delete_if_present || rc=1
  vm_delete_if_present || rc=1
  prov_network_down || rc=1
  return "$rc"
}

cmd_status() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would describe $VM_NAME and $LOAD_VM_NAME"
    return 0
  fi
  prov_status
  on_load_vm prov_status
}

_all_exit() {
  local code=$?
  trap - EXIT
  if [ "$DRY_RUN" = "1" ]; then
    if [ "${TEARDOWN:-0}" = "1" ]; then
      log "(dry-run) would delete $VM_NAME and $LOAD_VM_NAME on exit"
    else
      log "(dry-run) nothing was created; a real run leaves the VM up until you run 'down'"
    fi
    exit "$code"
  fi
  if [ "${TEARDOWN:-0}" = "1" ]; then
    [ "$code" -eq 0 ] || log "a step failed (status $code); tearing down $VM_NAME and $LOAD_VM_NAME because --teardown was set"
    cmd_down || log "automatic teardown failed; delete them manually with: PROVIDER=${PROVIDER} ./run-cloud.sh down"
  elif any_vm_exists; then
    log "VMs $VM_NAME and $LOAD_VM_NAME are running and billing. Delete them with: PROVIDER=${PROVIDER} ./run-cloud.sh down"
  fi
  exit "$code"
}

cmd_all() {
  trap _all_exit EXIT
  cmd_up
  cmd_sync
  cmd_setup
  cmd_run
  cmd_fetch
}

usage() {
  cat <<'EOF'
Usage: PROVIDER=<gcp|hetzner|digitalocean|aws> ./run-cloud.sh <command> [flags]

Commands:
  all       up -> sync -> setup -> run -> fetch (add --teardown to delete after)
  up        create the engine VM, the load VM, and the private network between them
  sync      push the local working tree to both VMs
  setup     install PostgreSQL 18, Node 24, pnpm, and Python 3 on the engine VM, Node 24, pnpm,
            and Python 3 on the load VM, and give the engine VM an SSH key for the load VM
  run       build and run the benchmark suite (detached, streamed back)
  logs      re-attach to a run in progress
  fetch     copy run directories back into the repo
  ssh       open an interactive shell on the engine VM
  ssh-load  open an interactive shell on the load VM
  status    show the state of both VMs
  down      delete both VMs and the private network

Flags: --yes (skip billing prompt), --teardown (delete on exit, even if a step fails),
       --dry-run (print the commands instead of running them).

Common env:
  VM_NAME (the load VM is VM_NAME-load unless LOAD_VM_NAME is set),
  MACHINE_TYPE, LOAD_MACHINE_TYPE, DISK_SIZE (GB),
  VM_MAX_HOURS (self-delete backstop, default 24; 0 disables; gcp/aws only),
  SSH_KEY (private key for hetzner/digitalocean/aws; the public key is <key>.pub),
  BENCH_PROFILE (cloud by default: 10,000,000 rows; smoke for a quick check),
  BENCH_MACHINE_LABEL, BENCH_LOAD_MACHINE_LABEL, BENCH_DURABILITIES, BENCH_WORKLOADS,
  BENCH_TARGET_RATES, BENCH_CONCURRENCY_LEVELS, BENCH_DATA_SIZE, BENCH_RUNS,
  BENCH_WARMUP_SECONDS, BENCH_MEASURE_SECONDS.

A cheap end-to-end check on any provider, then clean up:
  PROVIDER=hetzner BENCH_DATA_SIZE=10000 BENCH_WORKLOADS=point-select BENCH_TARGET_RATES=1000 \
    BENCH_RUNS=2 BENCH_WARMUP_SECONDS=1 BENCH_MEASURE_SECONDS=2 \
    ./run-cloud.sh all --yes --teardown
EOF
}

main() {
  local sub="${1:-help}"
  shift || true
  local arg
  for arg in "$@"; do
    case "$arg" in
      --yes) ASSUME_YES=1 ;;
      --teardown) TEARDOWN=1 ;;
      --dry-run) DRY_RUN=1 ;;
      *) die "unknown option: $arg" ;;
    esac
  done
  case "$sub" in
    help | -h | --help) usage; return 0 ;;
  esac
  prov_init
  case "$sub" in
    up) cmd_up ;;
    sync) cmd_sync ;;
    setup) cmd_setup ;;
    run) cmd_run ;;
    logs) cmd_logs ;;
    fetch) cmd_fetch ;;
    ssh) prov_ssh_interactive ;;
    ssh-load) on_load_vm prov_ssh_interactive ;;
    status) cmd_status ;;
    down) cmd_down ;;
    all) cmd_all ;;
    *) die "unknown command: $sub (try 'help')" ;;
  esac
}
