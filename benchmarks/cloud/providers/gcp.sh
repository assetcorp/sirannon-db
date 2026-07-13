#!/usr/bin/env bash
# shellcheck disable=SC2154  # DRY_RUN, VM_NAME, DISK_SIZE, MACHINE_LABEL come from common.sh
#
# Google Compute Engine driver. gcloud manages SSH keys and the connection, so
# this driver does not use the raw-ssh transport. c3-standard-8-lssd keeps a fixed
# Sapphire Rapids CPU and bundles local NVMe SSDs, so database I/O avoids the
# network-attached pd-balanced boot disk.

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

prov_create() {
  # Keep the required flags in the array so it is never empty; expanding an empty
  # array under 'set -u' aborts on the bash 3.2 that ships with macOS.
  local -a flags=(
    --project "$PROJECT" --zone "$ZONE"
    --machine-type "$MACHINE_TYPE"
    --image-family "$IMAGE_FAMILY" --image-project "$IMAGE_PROJECT"
    --boot-disk-size "${DISK_SIZE}GB" --boot-disk-type pd-balanced
  )
  [ -n "${MIN_CPU_PLATFORM:-}" ] && flags+=(--min-cpu-platform "$MIN_CPU_PLATFORM")
  _run gcloud compute instances create "$VM_NAME" "${flags[@]}"
}

prov_delete() {
  _run gcloud compute instances delete "$VM_NAME" --project "$PROJECT" --zone "$ZONE" --quiet
}

prov_status() {
  gcloud compute instances describe "$VM_NAME" --project "$PROJECT" --zone "$ZONE" \
    --format='value(name,status,machineType.scope(machineTypes),zone.scope(zones))' 2>/dev/null \
    || log "$VM_NAME not found"
}

# IAP_FLAG is left unquoted on purpose: it is either '--tunnel-through-iap' or
# empty, and an empty value must expand to no argument rather than an empty one.
# shellcheck disable=SC2086
prov_ssh() {
  _run gcloud compute ssh "$VM_NAME" --project "$PROJECT" --zone "$ZONE" $IAP_FLAG --command "$1"
}
# shellcheck disable=SC2086
prov_ssh_interactive() {
  _run gcloud compute ssh "$VM_NAME" --project "$PROJECT" --zone "$ZONE" $IAP_FLAG
}
# shellcheck disable=SC2086
prov_scp_up() {
  _run gcloud compute scp --project "$PROJECT" --zone "$ZONE" $IAP_FLAG "$1" "${VM_NAME}:$2"
}
# shellcheck disable=SC2086
prov_scp_down() {
  _run gcloud compute scp --recurse --project "$PROJECT" --zone "$ZONE" $IAP_FLAG "${VM_NAME}:$1" "$2"
}
