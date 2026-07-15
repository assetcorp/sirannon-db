#!/usr/bin/env bash
# shellcheck disable=SC2154

# shellcheck source=lib/raw-ssh.sh
. "$CLOUD_DIR/lib/raw-ssh.sh"

prov_init() {
  require_cmd hcloud
  [ "$DRY_RUN" = "1" ] || hcloud context active >/dev/null 2>&1 || [ -n "${HCLOUD_TOKEN:-}" ] \
    || die "hcloud not configured; run 'hcloud context create' or export HCLOUD_TOKEN"
  MACHINE_TYPE="${MACHINE_TYPE:-ccx33}"
  LOCAL_SSD_MODE="${LOCAL_SSD_MODE:-root}"
  IMAGE="${IMAGE:-ubuntu-24.04}"
  LOCATION="${HCLOUD_LOCATION:-fsn1}"
  SSH_USER="${SSH_USER:-root}"
  : "${MACHINE_LABEL:=Hetzner ${MACHINE_TYPE}, ${LOCATION}}"
  resolve_ssh_key
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
