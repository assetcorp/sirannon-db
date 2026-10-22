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
  : "${LOAD_MACHINE_TYPE:=$MACHINE_TYPE}"
  : "${LOAD_MACHINE_LABEL:=Hetzner ${LOAD_MACHINE_TYPE}, ${LOCATION}}"
  HCLOUD_NETWORK="${HCLOUD_NETWORK:-${ENGINE_VM_NAME}-net}"
  case "$LOCATION" in
    fsn1 | nbg1 | hel1) HCLOUD_DEFAULT_ZONE="eu-central" ;;
    ash) HCLOUD_DEFAULT_ZONE="us-east" ;;
    hil) HCLOUD_DEFAULT_ZONE="us-west" ;;
    sin) HCLOUD_DEFAULT_ZONE="ap-southeast" ;;
    *) HCLOUD_DEFAULT_ZONE="" ;;
  esac
  HCLOUD_NETWORK_ZONE="${HCLOUD_NETWORK_ZONE:-$HCLOUD_DEFAULT_ZONE}"
  [ -n "$HCLOUD_NETWORK_ZONE" ] || die "no network zone known for location $LOCATION; set HCLOUD_NETWORK_ZONE"
  resolve_ssh_key
}

prov_network_up() {
  if [ "$DRY_RUN" != "1" ] && hcloud network describe "$HCLOUD_NETWORK" >/dev/null 2>&1; then
    log "network $HCLOUD_NETWORK already exists"
    return 0
  fi
  log "create the private network $HCLOUD_NETWORK in $HCLOUD_NETWORK_ZONE"
  _run hcloud network create --name "$HCLOUD_NETWORK" --ip-range 10.77.0.0/16
  _run hcloud network add-subnet "$HCLOUD_NETWORK" --network-zone "$HCLOUD_NETWORK_ZONE" \
    --type cloud --ip-range 10.77.0.0/24
}

prov_network_down() {
  if [ "$DRY_RUN" != "1" ] && ! hcloud network describe "$HCLOUD_NETWORK" >/dev/null 2>&1; then
    return 0
  fi
  log "delete the private network $HCLOUD_NETWORK"
  local attempt
  for attempt in 1 2 3 4 5 6; do
    if _run hcloud network delete "$HCLOUD_NETWORK"; then
      return 0
    fi
    log "network delete attempt ${attempt} failed; a server may still be detaching, so retrying in 10s"
    sleep 10
  done
  log "could not delete network $HCLOUD_NETWORK; delete it with: hcloud network delete $HCLOUD_NETWORK"
  return 1
}

prov_private_ip() {
  [ "$DRY_RUN" = "1" ] && { echo "<private-ip of $VM_NAME>"; return 0; }
  hcloud server describe "$VM_NAME" -o json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["private_net"][0]["ip"])'
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
      --image "$IMAGE" --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --network "$HCLOUD_NETWORK"
    return 0
  fi
  hcloud ssh-key describe "$SSH_KEY_NAME" >/dev/null 2>&1 \
    || _run hcloud ssh-key create --name "$SSH_KEY_NAME" --public-key-from-file "$SSH_PUBKEY"
  _run hcloud server create --name "$VM_NAME" --type "$MACHINE_TYPE" \
    --image "$IMAGE" --location "$LOCATION" --ssh-key "$SSH_KEY_NAME" --network "$HCLOUD_NETWORK"
}

prov_delete() {
  _run hcloud server delete "$VM_NAME"
}

prov_lifetime_note() {
  log "hetzner has no auto-delete backstop; if this machine dies mid-run the server bills until 'down'"
}

prov_status() {
  hcloud server describe "$VM_NAME" 2>/dev/null || log "$VM_NAME not found"
}
