#!/usr/bin/env bash

set -euo pipefail

cd "$HOME/sirannon"
rm -f "$HOME/bench.done" "$HOME/bench.status"
: > "$HOME/bench.log"

nohup env "$@" bash benchmarks/cloud/remote-run.sh >> "$HOME/bench.log" 2>&1 &
echo $! > "$HOME/bench.pid"
echo "launched pid $(cat "$HOME/bench.pid"); log at ~/bench.log"
