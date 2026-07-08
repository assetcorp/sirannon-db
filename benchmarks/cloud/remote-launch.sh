#!/usr/bin/env bash
#
# Start remote-run.sh detached so the benchmark keeps running after the
# controlling SSH session closes. Any KEY=VALUE arguments become environment
# for the run. Records the pid so the orchestrator can follow the log.

set -euo pipefail

cd "$HOME/sirannon"
rm -f "$HOME/bench.done" "$HOME/bench.status"
: > "$HOME/bench.log"

nohup env "$@" bash benchmarks/cloud/remote-run.sh >> "$HOME/bench.log" 2>&1 &
echo $! > "$HOME/bench.pid"
echo "launched pid $(cat "$HOME/bench.pid"); log at ~/bench.log"
