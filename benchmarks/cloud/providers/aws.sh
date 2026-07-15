#!/usr/bin/env bash
# shellcheck disable=SC2154

# shellcheck source=lib/raw-ssh.sh
. "$CLOUD_DIR/lib/raw-ssh.sh"

CANONICAL_OWNER="099720109477"

prov_init() {
  require_cmd aws
  AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"
  export AWS_DEFAULT_REGION="$AWS_REGION"
  [ "$DRY_RUN" = "1" ] || aws sts get-caller-identity >/dev/null 2>&1 \
    || die "aws not authenticated; run 'aws configure'"
  MACHINE_TYPE="${MACHINE_TYPE:-m6id.2xlarge}"
  LOCAL_SSD_MODE="${LOCAL_SSD_MODE:-required}"
  SSH_USER="${SSH_USER:-ubuntu}"
  SG_NAME="${AWS_SG_NAME:-sirannon-bench-sg}"
  : "${MACHINE_LABEL:=AWS ${MACHINE_TYPE}, ${AWS_REGION}}"
  resolve_ssh_key
}

_aws_instance_id() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$VM_NAME" "Name=instance-state-name,Values=pending,running" \
    --query 'Reservations[].Instances[].InstanceId' --output text 2>/dev/null \
    | tr '\t' '\n' | awk 'NF { print; exit }'
}

prov_exists() {
  [ -n "$(_aws_instance_id)" ]
}

prov_ip() {
  [ "$DRY_RUN" = "1" ] && { echo "<server-ip>"; return 0; }
  [ -n "${SERVER_IP:-}" ] && { echo "$SERVER_IP"; return 0; }
  local id
  id="$(_aws_instance_id)"
  [ -n "$id" ] || die "instance $VM_NAME not found"
  SERVER_IP="$(aws ec2 describe-instances --instance-ids "$id" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text 2>/dev/null)"
  [ -n "$SERVER_IP" ] && [ "$SERVER_IP" != "None" ] || die "cannot resolve $VM_NAME public IP"
  echo "$SERVER_IP"
}

_aws_security_group() {
  local sg vpc myip
  sg="$(aws ec2 describe-security-groups --filters "Name=group-name,Values=$SG_NAME" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)"
  if [ -n "$sg" ] && [ "$sg" != "None" ]; then
    echo "$sg"
    return 0
  fi
  vpc="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' --output text 2>/dev/null)"
  [ -n "$vpc" ] && [ "$vpc" != "None" ] || die "no default VPC in $AWS_REGION; set AWS_SUBNET and AWS_SG_NAME"
  sg="$(aws ec2 create-security-group --group-name "$SG_NAME" \
    --description "Sirannon benchmark SSH" --vpc-id "$vpc" --query 'GroupId' --output text)"
  myip="$(curl -fsS https://checkip.amazonaws.com 2>/dev/null | tr -d '[:space:]')"
  aws ec2 authorize-security-group-ingress --group-id "$sg" \
    --protocol tcp --port 22 --cidr "${myip:-0.0.0.0}/32" >/dev/null
  echo "$sg"
}

_aws_latest_ami() {
  aws ec2 describe-images --owners "$CANONICAL_OWNER" \
    --filters "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
    "Name=state,Values=available" \
    --query 'sort_by(Images,&CreationDate)[-1].ImageId' --output text 2>/dev/null
}

prov_create() {
  if [ "$DRY_RUN" = "1" ]; then
    log "(dry-run) would import key pair '$SSH_KEY_NAME', ensure security group '$SG_NAME' (SSH from your IP), resolve the latest Ubuntu 24.04 AMI, then:"
    _run aws ec2 run-instances --image-id "<ami>" --instance-type "$MACHINE_TYPE" \
      --key-name "$SSH_KEY_NAME" --security-group-ids "<sg>" \
      --block-device-mappings "gp3 ${DISK_SIZE}GB root" \
      --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$VM_NAME}]"
    return 0
  fi
  aws ec2 describe-key-pairs --key-names "$SSH_KEY_NAME" >/dev/null 2>&1 \
    || _run aws ec2 import-key-pair --key-name "$SSH_KEY_NAME" \
      --public-key-material "fileb://$SSH_PUBKEY"
  local sg ami bdm
  sg="$(_aws_security_group)"
  ami="${AWS_AMI:-$(_aws_latest_ami)}"
  [ -n "$ami" ] && [ "$ami" != "None" ] || die "could not resolve Ubuntu 24.04 AMI in $AWS_REGION"
  bdm="[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${DISK_SIZE},\"VolumeType\":\"gp3\"}}]"
  local -a run_flags=(
    --image-id "$ami" --instance-type "$MACHINE_TYPE"
    --key-name "$SSH_KEY_NAME" --security-group-ids "$sg"
    --block-device-mappings "$bdm"
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$VM_NAME}]"
  )
  [ -n "${AWS_SUBNET:-}" ] && run_flags+=(--subnet-id "$AWS_SUBNET")
  _run aws ec2 run-instances "${run_flags[@]}" --query 'Instances[0].InstanceId' --output text
  log "wait for instance-running"
  _run aws ec2 wait instance-running \
    --filters "Name=tag:Name,Values=$VM_NAME" "Name=instance-state-name,Values=pending,running"
}

prov_delete() {
  if [ "$DRY_RUN" = "1" ]; then
    _run aws ec2 terminate-instances --instance-ids "<id-of $VM_NAME>"
    return 0
  fi
  local id
  id="$(_aws_instance_id)"
  [ -n "$id" ] || { log "$VM_NAME not found"; return 0; }
  _run aws ec2 terminate-instances --instance-ids "$id" \
    --query 'TerminatingInstances[0].CurrentState.Name' --output text
}

prov_status() {
  aws ec2 describe-instances --filters "Name=tag:Name,Values=$VM_NAME" \
    --query 'Reservations[].Instances[].[InstanceId,State.Name,InstanceType,PublicIpAddress]' \
    --output text 2>/dev/null || log "$VM_NAME not found"
}
