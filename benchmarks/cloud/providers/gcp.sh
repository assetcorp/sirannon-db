#!/usr/bin/env bash
# shellcheck disable=SC2154

prov_init() {
  require_cmd gcloud
  PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
  [ -n "${PROJECT:-}" ] || [ "$DRY_RUN" = "1" ] || die "no GCP project; export GCP_PROJECT or run 'gcloud config set project'"
  PROJECT="${PROJECT:-DRY_RUN_PROJECT}"
  ZONE="${GCP_ZONE:-us-central1-a}"
  MACHINE_TYPE="${MACHINE_TYPE:-c3-standard-8-lssd}"
  LOCAL_SSD_MODE="${LOCAL_SSD_MODE:-required}"
  IMAGE_FAMILY="${IMAGE_FAMILY:-ubuntu-2404-lts-amd64}"
  IMAGE_PROJECT="${IMAGE_PROJECT:-ubuntu-os-cloud}"
  IAP_FLAG=""
  [ "${USE_IAP:-0}" = "1" ] && IAP_FLAG="--tunnel-through-iap"
  : "${MACHINE_LABEL:=GCP ${MACHINE_TYPE}, ${ZONE}}"
}

prov_exists() {
  gcloud compute instances describe "$VM_NAME" --project "$PROJECT" --zone "$ZONE" >/dev/null 2>&1
}

_gcp_termination_time() {
  local at
  at=$(( $(date +%s) + VM_MAX_HOURS * 3600 ))
  date -u -r "$at" +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d "@$at" +%Y-%m-%dT%H:%M:%SZ
}

prov_create() {
  # Must never be empty: expanding an empty array under 'set -u' aborts on macOS's bash 3.2.
  local -a flags=(
    --project "$PROJECT" --zone "$ZONE"
    --machine-type "$MACHINE_TYPE"
    --image-family "$IMAGE_FAMILY" --image-project "$IMAGE_PROJECT"
    --boot-disk-size "${DISK_SIZE}GB" --boot-disk-type pd-balanced
  )
  [ -n "${MIN_CPU_PLATFORM:-}" ] && flags+=(--min-cpu-platform "$MIN_CPU_PLATFORM")
  if [ "$VM_MAX_HOURS" != "0" ]; then
    flags+=(--termination-time "$(_gcp_termination_time)" --instance-termination-action DELETE)
  fi
  _run gcloud compute instances create "$VM_NAME" "${flags[@]}"
}

_gcp_deadline_rfc3339() {
  local ts
  ts="$(gcloud compute instances describe "$VM_NAME" --project "$PROJECT" --zone "$ZONE" \
    --format='value(scheduling.terminationTime)' 2>/dev/null)"
  [ -n "$ts" ] || ts="$(gcloud compute instances describe "$VM_NAME" --project "$PROJECT" --zone "$ZONE" \
    --format='value(scheduling.terminationTimestamp)' 2>/dev/null)"
  printf '%s' "$ts"
}

prov_termination_epoch() {
  local ts
  ts="$(_gcp_deadline_rfc3339)"
  [ -n "$ts" ] || return 0
  python3 - "$ts" 2>/dev/null <<'PY'
import sys
from datetime import datetime
print(int(datetime.fromisoformat(sys.argv[1].strip().replace('Z', '+00:00')).timestamp()))
PY
}

prov_lifetime_note() {
  if [ "$VM_MAX_HOURS" = "0" ]; then
    log "self-delete backstop disabled (VM_MAX_HOURS=0); the VM bills until 'down'"
    return 0
  fi
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) the VM would delete itself ${VM_MAX_HOURS}h after creation"
    return 0
  fi
  local ts
  ts="$(_gcp_deadline_rfc3339)"
  if [ -n "$ts" ]; then
    log "the VM deletes itself at $ts; fetch results before then"
  else
    log "warning: $VM_NAME has no self-delete deadline; recreate it (down, then up) to arm the backstop"
  fi
}

prov_delete() {
  _run gcloud compute instances delete "$VM_NAME" --project "$PROJECT" --zone "$ZONE" --quiet
}

prov_status() {
  gcloud compute instances describe "$VM_NAME" --project "$PROJECT" --zone "$ZONE" \
    --format='value(name,status,machineType.scope(machineTypes),zone.scope(zones))' 2>/dev/null \
    || log "$VM_NAME not found"
}

# Keepalives detect a dropped connection within a minute; without them an idle stream (long seed
# or soak phases produce no output) is silently killed by the network and tail hangs forever.
# IAP_FLAG must stay unquoted: when empty it has to expand to no argument at all.
# shellcheck disable=SC2086
prov_ssh() {
  _run gcloud compute ssh "$VM_NAME" --project "$PROJECT" --zone "$ZONE" $IAP_FLAG --command "$1" \
    -- -o ServerAliveInterval=15 -o ServerAliveCountMax=4
}
# shellcheck disable=SC2086
prov_ssh_interactive() {
  _run gcloud compute ssh "$VM_NAME" --project "$PROJECT" --zone "$ZONE" $IAP_FLAG \
    -- -o ServerAliveInterval=15 -o ServerAliveCountMax=4
}
# shellcheck disable=SC2086
prov_scp_up() {
  _run gcloud compute scp --project "$PROJECT" --zone "$ZONE" $IAP_FLAG "$1" "${VM_NAME}:$2"
}
# shellcheck disable=SC2086
prov_scp_down() {
  _run gcloud compute scp --recurse --project "$PROJECT" --zone "$ZONE" $IAP_FLAG "${VM_NAME}:$1" "$2"
}
