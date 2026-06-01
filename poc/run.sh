#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Run the ZK session expiry proof-of-concept
# Usage: ./run.sh [N_NODES] [SESSION_TIMEOUT_MS]
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

N_NODES=${1:-20}
SESSION_TIMEOUT_MS=${2:-8000}

cd "$(dirname "$0")"

echo "Building and starting containers…"
N_NODES=$N_NODES SESSION_TIMEOUT_MS=$SESSION_TIMEOUT_MS \
  docker-compose up --build --abort-on-container-exit --exit-code-from poc 2>&1 | \
  grep -v "^poc_zookeeper\|^poc_toxiproxy"

echo ""
echo "Run again with: ./run.sh [N_NODES] [SESSION_TIMEOUT_MS]"
