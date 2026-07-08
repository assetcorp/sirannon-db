#!/usr/bin/env bash
# shellcheck disable=SC2154  # DRY_RUN, SSH_USER etc. come from common.sh and the driver
#
# Raw SSH transport shared by the IP-addressed providers (Hetzner, DigitalOcean,
# AWS). Each of those drivers provides prov_ip; this file provides the transport
# functions and resolves the local SSH key. GCP does not use this file because
# gcloud manages its own SSH connection.
#
# Host-key checking is relaxed on purpose: benchmark VMs are throwaway and get a
# fresh IP each time, so pinning host keys would only produce mismatch prompts.

SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=10)

resolve_ssh_key() {
  if [ -z "${SSH_KEY:-}" ]; then
    local candidate
    for candidate in "$HOME/.ssh/id_ed25519" "$HOME/.ssh/id_rsa"; do
      [ -f "$candidate" ] && { SSH_KEY="$candidate"; break; }
    done
  fi
  [ -n "${SSH_KEY:-}" ] && [ -f "$SSH_KEY" ] || die "no SSH private key found; set SSH_KEY=/path/to/key"
  SSH_PUBKEY="${SSH_PUBKEY:-${SSH_KEY}.pub}"
  [ -f "$SSH_PUBKEY" ] || die "public key $SSH_PUBKEY not found (set SSH_PUBKEY)"
  SSH_KEY_NAME="${SSH_KEY_NAME:-sirannon-bench}"
}

prov_ssh() {
  _run ssh "${SSH_OPTS[@]}" -i "$SSH_KEY" "${SSH_USER}@$(prov_ip)" "$1"
}
prov_ssh_interactive() {
  _run ssh "${SSH_OPTS[@]}" -i "$SSH_KEY" "${SSH_USER}@$(prov_ip)"
}
prov_scp_up() {
  _run scp "${SSH_OPTS[@]}" -i "$SSH_KEY" "$1" "${SSH_USER}@$(prov_ip):$2"
}
prov_scp_down() {
  _run scp -r "${SSH_OPTS[@]}" -i "$SSH_KEY" "${SSH_USER}@$(prov_ip):$1" "$2"
}
