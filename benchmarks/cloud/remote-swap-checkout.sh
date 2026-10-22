#!/usr/bin/env bash

set -euo pipefail

NEXT="$(cd "$(dirname "$0")/../.." && pwd)"
CURRENT="$(dirname "$NEXT")/sirannon"
RESULTS_REL="benchmarks/server/results"

for runs_rel in runs .smoke/runs; do
  [ -d "$CURRENT/$RESULTS_REL/$runs_rel" ] || continue
  for kept in "$CURRENT/$RESULTS_REL/$runs_rel"/*/; do
    kept="${kept%/}"
    [ -d "$kept" ] || continue
    target="$NEXT/$RESULTS_REL/$runs_rel/$(basename "$kept")"
    mkdir -p "$target"
    cp -Rp "$kept/." "$target/"
    echo "kept $runs_rel/$(basename "$kept") from the previous checkout"
  done
done

rm -rf "$CURRENT"
mv "$NEXT" "$CURRENT"
