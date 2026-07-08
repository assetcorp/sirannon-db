#!/usr/bin/env bash
#
# Provision a fixed, disclosed VM on the selected cloud, run the Sirannon Docker
# benchmark track on it, copy the results back into this repository, and tear the
# VM down. Absolute performance numbers only reproduce on one named machine, so
# every provider defaults to a dedicated-vCPU instance whose CPU is fixed and
# recorded with every result.
#
#   PROVIDER=gcp          ./run-cloud.sh all      # default
#   PROVIDER=hetzner      ./run-cloud.sh all
#   PROVIDER=digitalocean ./run-cloud.sh all
#   PROVIDER=aws          ./run-cloud.sh all
#
# Run './run-cloud.sh help' for commands, flags, and environment.

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
