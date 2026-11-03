#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if [ "$(id -u)" -eq 0 ]; then
    echo "Run deploy.sh as the intended non-root simulator user." >&2
    exit 1
fi

if ! command -v ansible-playbook >/dev/null 2>&1; then
    echo "ansible-playbook is required; install ansible-core first." >&2
    exit 1
fi
if ! command -v flock >/dev/null 2>&1; then
    echo "flock is required for safe owner-scoped deployments." >&2
    exit 1
fi
if [ "$#" -ne 0 ]; then
    echo "Usage: ./deploy.sh" >&2
    exit 2
fi
if ! command -v go >/dev/null 2>&1; then
    echo "Go 1.23+ is required to build the simulator; install Go first." >&2
    exit 1
fi

# CSMS/GWay-Remote convention: serialize owner deployment attempts.
state_parent="$HOME/.local/state"
state_dir="$state_parent/ocpp-simulator"
if [ -L "$state_parent" ] || [ -L "$state_dir" ] || [ -L "$state_dir/deploy.lock" ]; then
    echo "Unsafe simulator deployment state path" >&2
    exit 2
fi
(umask 077; mkdir -p "$state_dir")
(umask 077; : >> "$state_dir/deploy.lock")
exec 9<>"$state_dir/deploy.lock"
if ! flock -n 9; then
    echo "Another OCPP simulator deployment is already in progress" >&2
    exit 3
fi

export ANSIBLE_ROLES_PATH="$ROOT/ansible/roles"
started=$(date +%s)
status=0
ansible-playbook "$ROOT/ansible/playbooks/install.yml" -i localhost, -c local || status=$?
elapsed=$(($(date +%s) - started))
printf 'OCPP simulator deployment completed in %ds (exit %s)\n' "$elapsed" "$status"
exit "$status"
