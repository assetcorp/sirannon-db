#!/usr/bin/env bash
#
# Build the workspace and run the Sirannon Docker benchmark track on the VM. The
# engine and e2e suites both run Sirannon and Postgres in resource-capped Docker
# containers, so a reachable Docker daemon is required. The exit status reflects
# whether anything failed; it is written to ~/bench.status with ~/bench.done as
# the completion marker the orchestrator watches for.

set -uo pipefail

SUITES="${SUITES:-both}"
LABEL="${BENCH_MACHINE_LABEL:-GCP VM}"
REPO="$HOME/sirannon"
status=0

cd "$REPO" || { echo "repo not found at $REPO" >&2; exit 1; }
export PATH="$HOME/.local/bin:$PATH"
export BENCH_MACHINE_LABEL="$LABEL"
corepack enable >/dev/null 2>&1 || true

stamp() { printf '\n[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# Both suites drive resource-capped containers, so the Docker daemon must be
# reachable in this session. usermod adds the login user to the docker group in
# bootstrap, but the group only takes effect in a fresh login; the detached run
# inherits the launching session, so fail fast with a clear message rather than
# a confusing permission error deep in docker compose.
if ! docker ps >/dev/null 2>&1; then
  stamp "docker is not usable in this session; the Docker benchmark track needs it"
  echo 1 > "$HOME/bench.status"
  touch "$HOME/bench.done"
  exit 1
fi

# The e2e half drives the app containers with k6; run-e2e.ts hard-exits if k6 is
# absent, so check before spending build time when e2e is in scope.
case "$SUITES" in
  both | e2e)
    if ! command -v k6 >/dev/null 2>&1; then
      stamp "k6 is not installed; the e2e suite needs it (run setup, or use SUITES=engine)"
      echo 1 > "$HOME/bench.status"
      touch "$HOME/bench.done"
      exit 1
    fi
    ;;
esac

stamp "pnpm install"
pnpm install || status=1

stamp "build workspace"
pnpm build || status=1

case "$SUITES" in
  engine)
    stamp "engine suite (Sirannon vs Postgres, capped Docker containers)"
    ( cd packages/ts && pnpm bench:docker:engine ) || status=1
    ;;
  e2e)
    stamp "e2e suite (k6 load against Sirannon and Postgres app containers)"
    ( cd packages/ts && pnpm bench:docker:e2e ) || status=1
    ;;
  both)
    stamp "Docker benchmark track (engine suite, then e2e suite)"
    ( cd packages/ts && pnpm bench:docker ) || status=1
    ;;
  *)
    stamp "unknown SUITES value '$SUITES' (expected both, engine, or e2e)"
    status=1
    ;;
esac

stamp "finished with status $status"
echo "$status" > "$HOME/bench.status"
touch "$HOME/bench.done"
exit "$status"
