#!/usr/bin/env bash

status_file="$1"
shift
"$@"
code=$?
printf '%s\n' "$code" > "$status_file"
exit "$code"
