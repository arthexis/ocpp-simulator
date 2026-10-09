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

started=$(date +%s)
status=0
ansible-playbook "$ROOT/ansible/playbooks/install.yml" -i localhost, -c local "$@" || status=$?
elapsed=$(($(date +%s) - started))
printf 'OCPP simulator deployment completed in %ds (exit %s)\n' "$elapsed" "$status"
exit "$status"
