#!/bin/bash

set -e

function die() {
  echo "$@"
  exit 2
}

TMP_DIR=$(mktemp -d)
SSH_CONTROL="$TMP_DIR/ssh_control"
TEST=/usr/bin/test
SERVICE_USER=warehouse
PACKAGE_NAME=datums_warehouse

SSH_CMD=(ssh -oControlMaster=no -oControlPath="$SSH_CONTROL" "root@${HOST}")
SCP_CMD=(scp -oControlMaster=no -oControlPath="$SSH_CONTROL")

function cleanup() {
  set + e
  echo "Closing master SSH connection"
  "${SSH_CMD[@]}" -O stop

  echo "Removing temporary backup files"
  rm -r "$TMP_DIR"
}
trap cleanup EXIT

echo "Open master SSH connection"
echo "to ${HOST}, deploying to ${APP_DIR}"
ssh -o UserKnownHostsFile=~/.ssh/known_hosts -oControlMaster=yes -oControlPath="$SSH_CONTROL" -o ControlPersist=10 -n -N "root@${HOST}"
"${SSH_CMD[@]}" "${TEST} -d ${APP_DIR}" && echo "APP_DIR exists" || (echo "APP_DIR not correct"; exit 1)
"${SSH_CMD[@]}" "${TEST} -d ${PIP_CACHE}" && echo "PIP_CACHE exists" || (echo "PIP_CACHE not correct"; exit 1)
"${SSH_CMD[@]}" "systemctl stop warehouse" && echo "stopped warehouse service" || (echo "failed to stop warehouse service"; exit 1)
"${SSH_CMD[@]}" "rm -rf ${APP_DIR}/*"
"${SCP_CMD[@]}" dist/${PACKAGE_NAME}*.whl root@${HOST}:${APP_DIR}/ && echo "copied wheel." || (echo "copying wheel failed"; exit 1)
"${SSH_CMD[@]}" "cd ${APP_DIR}; python3 -m venv venv" && echo "created virtualenv for warehouse" || (echo "failed to create virtualenv"; exit 1)
"${SSH_CMD[@]}" "PIP_CACHE_DIR=${PIP_CACHE} ${APP_DIR}/venv/bin/pip install ${APP_DIR}/${PACKAGE_NAME}*.whl" && echo "installed datums warehouse" || (echo "failed to install datums warehouse"; exit 1)
"${SSH_CMD[@]}" "chown -R ${SERVICE_USER} ${APP_DIR}" && echo "gave ownership to ${SERVICE_USER}" || (echo "failed to change ownership"; exit 1)
"${SSH_CMD[@]}" "systemctl start warehouse" && echo "started warehouse service" || (echo "failed to restart warehouse service"; exit 1)

cleanup
trap EXIT