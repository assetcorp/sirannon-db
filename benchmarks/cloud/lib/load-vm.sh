#!/usr/bin/env bash

ENGINE_VM_NAME="$VM_NAME"
LOAD_VM_NAME="${LOAD_VM_NAME:-${VM_NAME}-load}"
LOAD_MACHINE_TYPE="${LOAD_MACHINE_TYPE:-}"
LOAD_MACHINE_LABEL="${BENCH_LOAD_MACHINE_LABEL:-}"
ENGINE_TO_LOAD_KEY='$HOME/.ssh/bench_load'

on_load_vm() {
  (
    VM_NAME="$LOAD_VM_NAME"
    MACHINE_TYPE="$LOAD_MACHINE_TYPE"
    MACHINE_LABEL="$LOAD_MACHINE_LABEL"
    SERVER_IP=""
    "$@"
  )
}

vm_create_if_missing() {
  if [ "$DRY_RUN" != "1" ] && prov_exists; then
    log "$VM_NAME already exists"
    return 0
  fi
  log "create $VM_NAME ($MACHINE_TYPE)"
  prov_create
}

vm_delete_if_present() {
  if [ "$DRY_RUN" != "1" ] && ! prov_exists; then
    log "$VM_NAME not found; nothing to delete"
    return 0
  fi
  log "delete $VM_NAME"
  prov_delete
}

any_vm_exists() {
  prov_exists 2>/dev/null || on_load_vm prov_exists 2>/dev/null
}

vm_ssh_user() {
  if [ "$DRY_RUN" = "1" ]; then
    printf '<user>\n'
    return 0
  fi
  prov_ssh 'id -un' </dev/null | tr -d '\r' | tail -n1
}

link_load_vm() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would give $ENGINE_VM_NAME an SSH key that reaches $LOAD_VM_NAME over the private network, then test it"
    return 0
  fi
  local load_ip load_user pubkey
  load_ip="$(on_load_vm prov_private_ip)"
  load_user="$(on_load_vm vm_ssh_user)"
  [ -n "$load_ip" ] && [ -n "$load_user" ] || die "could not read the private address or user of $LOAD_VM_NAME"
  log "give $ENGINE_VM_NAME an SSH key for ${load_user}@${load_ip}"
  pubkey="$(prov_ssh "test -f ${ENGINE_TO_LOAD_KEY} || ssh-keygen -q -t ed25519 -N '' -C sirannon-bench-engine -f ${ENGINE_TO_LOAD_KEY}; cat ${ENGINE_TO_LOAD_KEY}.pub" </dev/null | tr -d '\r' | tail -n1)"
  case "$pubkey" in
    ssh-ed25519\ *) ;;
    *) die "could not read the engine VM's public key" ;;
  esac
  on_load_vm prov_ssh "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && { grep -qxF '${pubkey}' ~/.ssh/authorized_keys || printf '%s\n' '${pubkey}' >> ~/.ssh/authorized_keys; }" </dev/null \
    || die "could not install the engine VM's key on $LOAD_VM_NAME"
  prov_ssh "ssh -i ${ENGINE_TO_LOAD_KEY} -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 ${load_user}@${load_ip} true" </dev/null \
    || die "$ENGINE_VM_NAME cannot reach $LOAD_VM_NAME at ${load_ip} over SSH; check the private network and firewall"
  log "$ENGINE_VM_NAME reaches $LOAD_VM_NAME at ${load_ip}"
}

load_vm_forward() {
  local engine_ip load_ip load_user
  engine_ip="$(prov_private_ip)"
  load_ip="$(on_load_vm prov_private_ip)"
  load_user="$(on_load_vm vm_ssh_user)"
  [ -n "$engine_ip" ] && [ -n "$load_ip" ] && [ -n "$load_user" ] \
    || die "could not read the private addresses of $ENGINE_VM_NAME and $LOAD_VM_NAME"
  printf '%q ' "BENCH_ENGINE_ADDRESS=${engine_ip}" "BENCH_LOAD_ADDRESS=${load_ip}" \
    "BENCH_LOAD_USER=${load_user}" "BENCH_LOAD_MACHINE_LABEL=${LOAD_MACHINE_LABEL}"
}
