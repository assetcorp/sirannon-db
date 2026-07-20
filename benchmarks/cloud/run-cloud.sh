#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROVIDER="${PROVIDER:-gcp}"
DRIVER="$SCRIPT_DIR/providers/${PROVIDER}.sh"

[ -f "$DRIVER" ] || {
  printf 'unknown PROVIDER %q (expected gcp, hetzner, digitalocean, or aws)\n' "$PROVIDER" >&2
  exit 1
}

# shellcheck source=lib/common.sh
. "$SCRIPT_DIR/lib/common.sh"
# shellcheck source=/dev/null
. "$DRIVER"

main "$@"
