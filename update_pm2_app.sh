#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROCESS_NAME="$(basename "$REPO_DIR")"
PM2_PROCESS_EXISTS=0

cd "$REPO_DIR"

if pm2 describe "$PROCESS_NAME" >/dev/null 2>&1; then
  PM2_PROCESS_EXISTS=1
fi

if [[ "$PM2_PROCESS_EXISTS" -eq 0 ]]; then
  echo "pm2 process '$PROCESS_NAME' not found."
  exit 1
fi

on_error() {
  if [[ "$PM2_PROCESS_EXISTS" -eq 1 ]]; then
    pm2 restart "$PROCESS_NAME" >/dev/null 2>&1 || true
  fi
}

trap on_error ERR

if [[ "$PM2_PROCESS_EXISTS" -eq 1 ]]; then
  pm2 stop "$PROCESS_NAME"
fi

git pull --ff-only

if [[ "$PM2_PROCESS_EXISTS" -eq 1 ]]; then
  pm2 restart "$PROCESS_NAME"
fi

trap - ERR