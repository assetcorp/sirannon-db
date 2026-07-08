#!/usr/bin/env bash
# shellcheck disable=SC2154  # DRY_RUN, VM_NAME, MACHINE_LABEL, CLOUD_DIR come from common.sh
#
# Hetzner Cloud driver. The CCX line is dedicated AMD EPYC with no noisy
# neighbour, the cheapest fixed-CPU option here. Storage is included with the
# server type, so DISK_SIZE does not apply.

# shellcheck source=lib/raw-ssh.sh
. "$CLOUD_DIR/lib/raw-ssh.sh"

prov_init() {
  require_cmd hcloud
  [ "$DRY_RUN" = "1" ] || hcloud context active >/dev/null 2>&1 || [ -n "${HCLOUD_TOKEN:-}" ] \
    || die "hcloud not configured; run 'hcloud context create' or export HCLOUD_TOKEN"
  MACHINE_TYPE="${MACHINE_TYPE:-ccx33}"
  IMAGE="${IMAGE:-ubuntu-24.04}"
  LOCATION="${HCLOUD_LOCATION:-fsn1}"
  SSH_USER="${SSH_USER:-root}"
  : "${MACHINE_LABEL:=Hetzner ${MACHINE_TYPE}, ${LOCATION}}"
  resolve_ssh_key
}

prov_hourly() {
  case "$MACHINE_TYPE" in
    ccx33) echo "~\$0.25" ;;
    ccx43) echo "~\$0.51" ;;
    *) echo "unknown" ;;
  esac
}

prov_exists() {
  hcloud server describe "$VM_NAME" >/dev/null 2>&1
}

prov_ip() {
  [ "$DRY_RUN" = "1" ] && { echo "<server-ip>"; return 0; }
  [ -n "${SERVER_IP:-}" ] && { echo "$SERVER_IP"; return 0; }
  SERVER_IP="$(hcloud server ip "$VM_NAME" 2>/dev/null)"
  [ -n "$SERVER_IP" ] || die "cannot resolve $VM_NAME public IP"
  echo "$SERVER_IP"
}

prov_create() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would register ssh key '$SSH_KEY_NAME' and create the server"
    _run hcloud server create --name "$VM_NAME" --type "$MACHINE_TYPE" \
      --image "$IMAGE" --location "$LOCATION" --ssh-key "$SSH_KEY_NAME"
    return 0
  fi
  hcloud ssh-key describe "$SSH_KEY_NAME" >/dev/null 2>&1 \
    || _run hcloud ssh-key create --name "$SSH_KEY_NAME" --public-key-from-file "$SSH_PUBKEY"
  _run hcloud server create --name "$VM_NAME" --type "$MACHINE_TYPE" \
    --image "$IMAGE" --location "$LOCATION" --ssh-key "$SSH_KEY_NAME"
}

prov_delete() {
  _run hcloud server delete "$VM_NAME"
}

prov_status() {
  hcloud server describe "$VM_NAME" 2>/dev/null || log "$VM_NAME not found"
}
