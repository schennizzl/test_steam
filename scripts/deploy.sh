#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-/opt/wather}"
BRANCH="${2:-main}"

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "[deploy] repo not found: $REPO_DIR" >&2
  exit 1
fi

cd "$REPO_DIR"

echo "[deploy] repo=$REPO_DIR branch=$BRANCH"

# Prevent overlapping deploy runs on the server.
exec 9>"/tmp/wather-deploy.lock"
if ! flock -n 9; then
  echo "[deploy] another deploy is running" >&2
  exit 1
fi

git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

docker compose up -d --build --remove-orphans

echo "[deploy] done"
