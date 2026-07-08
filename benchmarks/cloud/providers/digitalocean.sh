#!/usr/bin/env bash
# shellcheck disable=SC2154  # DRY_RUN, VM_NAME, MACHINE_LABEL, CLOUD_DIR come from common.sh
#
# DigitalOcean driver. The general-purpose g-8vcpu-32gb droplet gives 8 dedicated
# vCPU and 32 GB, matching the other providers. doctl addresses droplets by id,
# so name is resolved to an id for ip, status, and delete.

# shellcheck source=lib/raw-ssh.sh
. "$CLOUD_DIR/lib/raw-ssh.sh"

prov_init() {
  require_cmd doctl
  [ "$DRY_RUN" = "1" ] || doctl account get >/dev/null 2>&1 \
    || die "doctl not authenticated; run 'doctl auth init'"
  MACHINE_TYPE="${MACHINE_TYPE:-g-8vcpu-32gb}"
  IMAGE="${IMAGE:-ubuntu-24-04-x64}"
  REGION="${DO_REGION:-nyc3}"
  SSH_USER="${SSH_USER:-root}"
  : "${MACHINE_LABEL:=DigitalOcean ${MACHINE_TYPE}, ${REGION}}"
  resolve_ssh_key
}

prov_hourly() {
  case "$MACHINE_TYPE" in
    g-8vcpu-32gb) echo "~\$0.24" ;;
    *) echo "unknown" ;;
  esac
}

_do_id() {
  doctl compute droplet list --format ID,Name --no-header 2>/dev/null \
    | awk -v n="$VM_NAME" '$2 == n { print $1; exit }'
}

prov_exists() {
  [ -n "$(_do_id)" ]
}

prov_ip() {
  [ "$DRY_RUN" = "1" ] && { echo "<server-ip>"; return 0; }
  [ -n "${SERVER_IP:-}" ] && { echo "$SERVER_IP"; return 0; }
  local id
  id="$(_do_id)"
  [ -n "$id" ] || die "droplet $VM_NAME not found"
  SERVER_IP="$(doctl compute droplet get "$id" --format PublicIPv4 --no-header 2>/dev/null)"
  [ -n "$SERVER_IP" ] || die "cannot resolve $VM_NAME public IP"
  echo "$SERVER_IP"
}

prov_create() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would import ssh key '$SSH_KEY_NAME' and create the droplet"
    _run doctl compute droplet create "$VM_NAME" --size "$MACHINE_TYPE" \
      --image "$IMAGE" --region "$REGION" --ssh-keys "<fingerprint>" --wait
    return 0
  fi
  doctl compute ssh-key list --format Name --no-header 2>/dev/null | grep -qx "$SSH_KEY_NAME" \
    || _run doctl compute ssh-key import "$SSH_KEY_NAME" --public-key-file "$SSH_PUBKEY"
  local fingerprint
  fingerprint="$(doctl compute ssh-key list --format Name,FingerPrint --no-header 2>/dev/null \
    | awk -v n="$SSH_KEY_NAME" '$1 == n { print $2; exit }')"
  [ -n "$fingerprint" ] || die "could not resolve DigitalOcean ssh-key fingerprint for $SSH_KEY_NAME"
  _run doctl compute droplet create "$VM_NAME" --size "$MACHINE_TYPE" \
    --image "$IMAGE" --region "$REGION" --ssh-keys "$fingerprint" --wait
}

prov_delete() {
  if [ "$DRY_RUN" = "1" ]; then
    _run doctl compute droplet delete "$VM_NAME" -f
    return 0
  fi
  local id
  id="$(_do_id)"
  [ -n "$id" ] || { log "$VM_NAME not found"; return 0; }
  _run doctl compute droplet delete "$id" -f
}

prov_status() {
  doctl compute droplet list --format Name,Status,Region,Memory,VCPUs --no-header 2>/dev/null \
    | awk -v n="$VM_NAME" '$1 == n' || log "$VM_NAME not found"
}
