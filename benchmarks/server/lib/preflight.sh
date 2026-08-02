#!/usr/bin/env bash

PG_BINDIR="/usr/lib/postgresql/17/bin"

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

require_supported_host() {
  if [ "$(uname -s)" != "Linux" ] || ! command -v systemd-run >/dev/null 2>&1; then
    echo "this harness pins engines with systemd cgroups and needs Linux with systemd-run; run it on the benchmark VM through benchmarks/cloud/" >&2
    exit 2
  fi
  if [ -z "${EPOCHREALTIME:-}" ]; then
    echo "bash 5 or newer is required; EPOCHREALTIME drives the millisecond timers" >&2
    exit 2
  fi
  local controller required
  for controller in cpuset memory; do
    if ! grep -qw "$controller" /sys/fs/cgroup/cgroup.controllers 2>/dev/null; then
      echo "the cgroup v2 ${controller} controller is unavailable; the resource caps cannot be enforced" >&2
      exit 2
    fi
  done
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
