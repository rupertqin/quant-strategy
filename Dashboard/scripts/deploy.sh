#!/bin/bash
set -e

cd "$(dirname "$0")/.."

set -a
source ./.env
set +a

rsync -avz --delete ./dist/ "${DEPLOY_USER}@${DEPLOY_IP}:${DEPLOY_PATH}"
