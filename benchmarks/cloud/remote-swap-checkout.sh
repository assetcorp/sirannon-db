#!/usr/bin/env bash

set -euo pipefail

NEXT="$(cd "$(dirname "$0")/../.." && pwd)"
CURRENT="$(dirname "$NEXT")/sirannon"
RUNS_REL="benchmarks/server/results/runs"

if [ -d "$CURRENT/$RUNS_REL" ] && [ -d "$NEXT/$RUNS_REL" ]; then
  for pushed in "$NEXT/$RUNS_REL"/*/; do
    pushed="${pushed%/}"
    [ -d "$pushed" ] || continue
    previous="$CURRENT/$RUNS_REL/$(basename "$pushed")"
    [ -d "$previous" ] || continue
    while IFS= read -r -d '' file; do
      if [ ! -e "$pushed/$file" ]; then
        mkdir -p "$(dirname "$pushed/$file")"
        cp -p "$previous/$file" "$pushed/$file"
        echo "kept $(basename "$pushed")/${file#./} from the previous checkout"
      fi
    done < <(cd "$previous" && find . -type f -print0)
  done
fi

rm -rf "$CURRENT"
mv "$NEXT" "$CURRENT"
