#!/usr/bin/env bash
#
# Idempotent VM setup: Docker, git, Python 3, plus database data on local NVMe.
# Both engines write into their container layer, so Docker's data-root moves to
# the NVMe mount instead of bind-mounting each database. A provider whose root is
# already local NVMe has no separate device; BENCH_LOCAL_SSD_MODE=required makes a
# missing local disk a hard failure rather than a silent fall back to slow disk.

set -euo pipefail

log() { printf '[bootstrap] %s\n' "$*"; }

SSD_MODE="${BENCH_LOCAL_SSD_MODE:-auto}"
NVME_MOUNT="/mnt/nvme"

root_disk() {
  local src
  src="$(findmnt -no SOURCE / 2>/dev/null || true)"
  [ -n "$src" ] || return 0
  lsblk -no PKNAME "$src" 2>/dev/null | head -n1
}

find_spare_nvme() {
  local rootdisk name type mnt
  rootdisk="$(root_disk)"
  while read -r name type; do
    [ "$type" = "disk" ] || continue
    [ "$name" != "$rootdisk" ] || continue
    case "$name" in nvme*) : ;; *) continue ;; esac
    mnt="$(lsblk -no MOUNTPOINTS "/dev/$name" 2>/dev/null | tr -d '[:space:]')"
    [ -z "$mnt" ] || continue
    printf '/dev/%s\n' "$name"
    return 0
  done < <(lsblk -dno NAME,TYPE 2>/dev/null)
  return 1
}

setup_local_nvme() {
  if findmnt -no TARGET "$NVME_MOUNT" >/dev/null 2>&1; then
    log "local NVMe already mounted at $NVME_MOUNT"
    return 0
  fi
  local dev
  if dev="$(find_spare_nvme)"; then
    log "local NVMe $dev found; formatting and mounting at $NVME_MOUNT"
    sudo mkfs.ext4 -F -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$dev"
    sudo mkdir -p "$NVME_MOUNT"
    sudo mount -o discard,defaults "$dev" "$NVME_MOUNT"
    sudo mkdir -p "$NVME_MOUNT/docker" /etc/docker
    printf '{\n  "data-root": "%s/docker"\n}\n' "$NVME_MOUNT" | sudo tee /etc/docker/daemon.json >/dev/null
    log "Docker data-root set to $NVME_MOUNT/docker"
  elif [ "$SSD_MODE" = "required" ]; then
    log "no local NVMe device found, but this provider must supply one; refusing to run database I/O on a network disk"
    exit 1
  else
    log "no separate NVMe device; the root disk is local NVMe, so Docker keeps its default data-root"
  fi
}

log "apt packages"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  ca-certificates curl gnupg git python3

log "local NVMe (mode: $SSD_MODE)"
setup_local_nvme

if ! command -v docker >/dev/null 2>&1; then
  log "install Docker"
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER"

log "done"
