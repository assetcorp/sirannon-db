#!/usr/bin/env bash
#
# One-time (idempotent) setup on the benchmark VM: Docker, Node 22, pnpm, and k6.
# Node 22 matches both the engine's declared floor and the node:22 base image the
# benchmark containers build from, so the host that drives the run matches what
# it measures. Safe to re-run.

set -euo pipefail

NODE_MAJOR=22
PNPM_VERSION="11.8.0"

log() { printf '[bootstrap] %s\n' "$*"; }

log "apt packages"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  ca-certificates curl gnupg git build-essential python3

if ! command -v docker >/dev/null 2>&1; then
  log "install Docker"
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER"

current_node="$(node -v 2>/dev/null | sed 's/v\([0-9]*\).*/\1/' || echo none)"
if [ "$current_node" != "$NODE_MAJOR" ]; then
  log "install Node $NODE_MAJOR"
  curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | sudo -E bash -
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nodejs
fi

log "enable pnpm $PNPM_VERSION through corepack"
sudo corepack enable
corepack prepare "pnpm@${PNPM_VERSION}" --activate || true

if ! command -v k6 >/dev/null 2>&1; then
  log "install k6 (Grafana apt repository)"
  sudo gpg -k >/dev/null 2>&1
  curl -fsSL https://dl.k6.io/key.gpg | sudo gpg --dearmor --yes -o /usr/share/keyrings/k6-archive-keyring.gpg
  echo 'deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main' \
    | sudo tee /etc/apt/sources.list.d/k6.list >/dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq k6
fi

log "done"
