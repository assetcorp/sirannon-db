#!/usr/bin/env bash

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
  elif [ "$SSD_MODE" = "required" ]; then
    log "no local NVMe device found, but this provider must supply one; refusing to run database I/O on a network disk"
    exit 1
  else
    log "no separate NVMe device; the root disk is local NVMe, so database data stays on it"
  fi
}

log "apt packages"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  ca-certificates curl gnupg git python3 build-essential postgresql-common

log "local NVMe (mode: $SSD_MODE)"
setup_local_nvme

# A packaged default cluster would hold port 5432 and race the benchmark's own postgres.
sudo mkdir -p /etc/postgresql-common/createcluster.d
echo "create_main_cluster = false" | sudo tee /etc/postgresql-common/createcluster.d/no-main-cluster.conf >/dev/null

if [ ! -x /usr/lib/postgresql/17/bin/postgres ]; then
  log "install PostgreSQL 17 (PGDG apt repository)"
  sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-17
  sudo apt-mark hold postgresql-17 >/dev/null
fi
if pg_lsclusters 2>/dev/null | awk '{print $1, $2}' | grep -q '^17 main$'; then
  log "drop the packaged default cluster"
  sudo pg_dropcluster --stop 17 main
fi

if ! command -v node >/dev/null 2>&1 || [ "$(node -v | cut -d. -f1)" != "v24" ]; then
  log "install Node 24 (NodeSource apt repository)"
  sudo install -d -m 755 /etc/apt/keyrings
  curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
    | sudo gpg --dearmor --yes -o /etc/apt/keyrings/nodesource.gpg
  echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_24.x nodistro main" \
    | sudo tee /etc/apt/sources.list.d/nodesource.list >/dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nodejs
fi

log "enable pnpm (pinned by the repo's packageManager field)"
if command -v corepack >/dev/null 2>&1; then
  sudo corepack enable
  if [ -f "$HOME/sirannon/package.json" ]; then
    (cd "$HOME/sirannon" && COREPACK_ENABLE_DOWNLOAD_PROMPT=0 corepack install)
  fi
else
  pnpm_version="$(python3 -c "import json; print(json.load(open('$HOME/sirannon/package.json'))['packageManager'].split('@')[1].split('+')[0])")"
  sudo npm install -g "pnpm@${pnpm_version}"
fi

log "done"
