#!/usr/bin/env bash

drop_caches() {
  sync
  printf '3\n' | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

cooldown() {
  echo "cooldown: sync, settle dirty pages (<= ${COOLDOWN_DIRTY_KB} kB), trim, pause ${COOLDOWN_SECONDS}s"
  sync
  local waited=0 dirty
  while :; do
    dirty="$(awk '/^(Dirty|Writeback):/ { total += $2 } END { print total + 0 }' /proc/meminfo)"
    [ "$dirty" -le "$COOLDOWN_DIRTY_KB" ] && break
    if [ "$waited" -ge "$COOLDOWN_TIMEOUT" ]; then
      echo "dirty pages still at ${dirty} kB after ${COOLDOWN_TIMEOUT}s; continuing" >&2
      break
    fi
    sleep 1
    waited=$((waited + 1))
  done
  sudo fstrim "$(findmnt -no TARGET -T "$BENCH_DATA_ROOT")" >/dev/null 2>&1 || true
  sleep "$COOLDOWN_SECONDS"
}
