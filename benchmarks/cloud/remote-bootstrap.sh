#!/usr/bin/env bash
#
# One-time (idempotent) setup on the benchmark VM: Docker, git, and Python 3.
# The benchmark images build Node, pnpm, and the Sirannon package inside the
# containers, so the host itself needs only Docker to run them, git to unpack
# the working tree, and Python 3 to drive the orchestration and the writeup.
# Safe to re-run.

set -euo pipefail

log() { printf '[bootstrap] %s\n' "$*"; }

log "apt packages"
sudo DEBIAN_FRONTEND=noninteractive apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
  ca-certificates curl gnupg git python3

if ! command -v docker >/dev/null 2>&1; then
  log "install Docker"
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER"

log "done"
