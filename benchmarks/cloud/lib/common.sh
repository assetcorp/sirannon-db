#!/usr/bin/env bash
#
# Shared orchestration for the cloud benchmark runners. A provider driver,
# sourced after this file, supplies the provider-specific pieces: prov_init,
# prov_exists, prov_create, prov_delete, prov_status, prov_hourly, and the
# transport prov_ssh, prov_ssh_interactive, prov_scp_up, and prov_scp_down.
# Everything below is identical across providers: git packaging, the detached
# run, result fetching, dry-run, and teardown.

CLOUD_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$CLOUD_DIR/.." && pwd)"

PROVIDER="${PROVIDER:-gcp}"
VM_NAME="${VM_NAME:-sirannon-bench}"
DISK_SIZE="${DISK_SIZE:-60}"
SUITES="${SUITES:-both}"
DRY_RUN="${DRY_RUN:-0}"
MACHINE_LABEL="${BENCH_MACHINE_LABEL:-}"
MACHINE_TYPE="${MACHINE_TYPE:-}"

# The Docker benchmark run output lands here as flat, timestamped files (git
# ignores the JSON, tracks the CSV), so the whole directory is the fetch target.
RESULTS_REL="packages/ts/benchmarks/results"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

# Single choke point for every side-effecting external command, so --dry-run can
# show the plan without touching the cloud.
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
  if [ "$DRY_RUN" != "1" ] && prov_exists; then
    log "$VM_NAME already exists"
    return 0
  fi
  log "create $VM_NAME ($MACHINE_TYPE, $(prov_hourly)/hr)"
  if [ "$DRY_RUN" != "1" ] && [ "${ASSUME_YES:-0}" != "1" ]; then
    read -r -p "This starts billing until you run 'down'. Proceed? [y/N] " reply
    [ "$reply" = "y" ] || [ "$reply" = "Y" ] || die "aborted"
  fi
  prov_create
  prov_wait_ssh
}

# Package exactly what git tracks or leaves untracked-but-not-ignored, plus the
# .git directory so the run can stamp the build commit. Delegating the file set
# to git keeps this identical on macOS and Linux and never drifts from
# .gitignore, so node_modules, dist, and cached datasets are excluded because
# git already ignores them. Committed result files still ship (they match HEAD,
# so they stay clean), but untracked result files are skipped: they are
# generated output, and shipping a stray local run would show up as a dirty
# working tree on the VM and pollute the git status each run records.
cmd_sync() {
  command -v git >/dev/null 2>&1 || die "git is required to package the working tree"
  log "package the working tree via git (honours .gitignore, keeps .git for the build stamp)"
  if [ "$DRY_RUN" = "1" ]; then
    prov_scp_up "<working-tree>.tgz" "sirannon.tgz"
    prov_ssh "unpack sirannon.tgz into ~/sirannon"
    return 0
  fi
  local tarball filelist
  tarball="$(mktemp "${TMPDIR:-/tmp}/sirannon-sync.XXXXXX")"
  filelist="$(mktemp "${TMPDIR:-/tmp}/sirannon-files.XXXXXX")"
  {
    git -C "$REPO_ROOT" ls-files -z
    git -C "$REPO_ROOT" ls-files --others --exclude-standard -z \
      -- . ":(exclude)${RESULTS_REL}"
    printf '.git\0'
  } >"$filelist"
  # COPYFILE_DISABLE stops macOS tar from embedding AppleDouble '._*' sidecar
  # files (they carry extended attributes like com.apple.provenance). On the
  # Linux VM those extract as untracked junk and show up as a dirty working tree,
  # polluting the git status each run records. The variable is a no-op on GNU tar.
  COPYFILE_DISABLE=1 tar czf "$tarball" -C "$REPO_ROOT" --null -T "$filelist"
  rm -f "$filelist"
  log "upload and unpack to ~/sirannon"
  prov_scp_up "$tarball" "sirannon.tgz"
  prov_ssh "rm -rf sirannon && mkdir -p sirannon && tar xzf sirannon.tgz -C sirannon && rm -f sirannon.tgz"
  rm -f "$tarball"
}

cmd_setup() {
  log "install Docker, Node, pnpm, and k6 on the VM"
  prov_ssh "bash sirannon/benchmarks/cloud/remote-bootstrap.sh"
}

cmd_run() {
  log "launch the benchmark run (detached; survives an SSH drop)"
  local forward
  forward="$(printf '%q ' "SUITES=$SUITES" "BENCH_MACHINE_LABEL=${MACHINE_LABEL}")"
  local v
  for v in BENCH_DURABILITY BENCH_DATA_SIZES BENCH_WORKLOADS \
    BENCH_WARMUP_MS BENCH_MEASURE_MS BENCH_CONCURRENCY_LEVELS \
    BENCH_DATA_SIZE BENCH_RPS_LEVELS BENCH_DURATION \
    BENCH_CPUS BENCH_MEMORY; do
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
  # The $(cat bench.pid) must expand on the VM, not here, so tail follows the
  # remote run's pid; single quotes keep it unexpanded on the control host.
  # shellcheck disable=SC2016
  prov_ssh 'tail -n +1 --follow=name --pid=$(cat bench.pid 2>/dev/null || echo 1) bench.log' || true
  local st
  st="$(prov_ssh 'cat bench.status 2>/dev/null || echo running')"
  case "$st" in
    0) log "run finished cleanly" ;;
    running) log "still running (you detached); re-attach with: PROVIDER=$PROVIDER run-cloud.sh logs" ;;
    *) log "run reported failures (status $st); inspect with: PROVIDER=$PROVIDER run-cloud.sh ssh" ;;
  esac
}

cmd_fetch() {
  local remote="$RESULTS_REL" dest="$REPO_ROOT/$RESULTS_REL"
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would fetch new result files and run directories from $remote"
    return 0
  fi
  local files
  # -maxdepth 1 keeps generated chart SVGs (under results/charts) and the
  # per-run directories (under results/runs) out of this pass; only the
  # top-level timestamped JSON and CSV files are copied here.
  files="$(prov_ssh "find sirannon/$remote -maxdepth 1 -type f \( -name '*.json' -o -name '*.csv' \) -printf '%f\n' 2>/dev/null || true" | tr -d '\r')"
  if [ -n "$files" ]; then
    mkdir -p "$dest"
    local name
    for name in $files; do
      if [ -e "$dest/$name" ]; then
        log "have $name already, skipping"
        continue
      fi
      log "fetch $name"
      prov_scp_down "sirannon/$remote/$name" "$dest/"
    done
  else
    log "no top-level result files under $remote yet"
  fi

  # Each self-describing run is a directory keyed by a compact UTC run id, holding
  # run.json plus its result files. The writeup generator reads the latest committed
  # run, so the whole run directory has to come back intact to publish numbers.
  local runs
  runs="$(prov_ssh "find sirannon/$remote/runs -mindepth 1 -maxdepth 1 -type d -printf '%f\n' 2>/dev/null || true" | tr -d '\r')"
  if [ -n "$runs" ]; then
    mkdir -p "$dest/runs"
    local run
    for run in $runs; do
      if [ -e "$dest/runs/$run" ]; then
        log "have run $run already, skipping"
        continue
      fi
      log "fetch run $run"
      prov_scp_down "sirannon/$remote/runs/$run" "$dest/runs/"
    done
  else
    log "no run directories under $remote/runs yet"
  fi

  log "results copied under $RESULTS_REL/ in this repo (not committed; commit the ones you publish)"
}

cmd_down() {
  if [ "$DRY_RUN" != "1" ] && ! prov_exists; then
    log "$VM_NAME not found; nothing to delete"
    return 0
  fi
  log "delete $VM_NAME"
  prov_delete
}

cmd_status() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would describe $VM_NAME"
    return 0
  fi
  prov_status
}

cmd_all() {
  cmd_up
  cmd_sync
  cmd_setup
  cmd_run
  cmd_fetch
  if [ "${TEARDOWN:-0}" = "1" ]; then
    cmd_down
  elif [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) nothing was created; a real run would leave the VM up until you run 'down'"
  else
    log "VM left running and billing. Delete it with: PROVIDER=${PROVIDER} run-cloud.sh down"
  fi
}

usage() {
  cat <<'EOF'
Usage: PROVIDER=<gcp|hetzner|digitalocean|aws> ./run-cloud.sh <command> [flags]

Commands:
  all      up -> sync -> setup -> run -> fetch (add --teardown to delete after)
  up       create the VM
  sync     push the local working tree
  setup    install Docker, Node, pnpm, and k6 on the VM
  run      build and run the Docker benchmark track (detached, streamed back)
  logs     re-attach to a run in progress
  fetch    copy result files back into the repo
  ssh      open an interactive shell on the VM
  status   show the VM state
  down     delete the VM

Flags: --yes (skip billing prompt), --teardown (delete on success),
       --dry-run (print the commands instead of running them).

Common env:
  VM_NAME, MACHINE_TYPE, DISK_SIZE (GB), SUITES (both|engine|e2e),
  SSH_KEY (private key for hetzner/digitalocean/aws; the public key is <key>.pub),
  BENCH_MACHINE_LABEL, BENCH_DURABILITY, BENCH_DATA_SIZES, BENCH_WORKLOADS,
  BENCH_DATA_SIZE, BENCH_RPS_LEVELS, BENCH_DURATION, BENCH_CPUS, BENCH_MEMORY.

A cheap end-to-end smoke on any provider, then clean up:
  PROVIDER=hetzner SUITES=engine BENCH_DATA_SIZES=1000 BENCH_WORKLOADS=point-select \
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
    status) cmd_status ;;
    down) cmd_down ;;
    all) cmd_all ;;
    *) die "unknown command: $sub (try 'help')" ;;
  esac
}
