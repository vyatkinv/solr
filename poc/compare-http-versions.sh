#!/usr/bin/env bash
# Сравнивает HTTP/1.1 vs HTTP/2: показывает разницу в числе TCP-соединений
# под одинаковой нагрузкой.
#
# Запускать после 'docker compose up -d' и './setup-collection.sh'.
#
# Использование:
#   ./compare-http-versions.sh

set -euo pipefail

SOLR=http://localhost:8983
LOAD_RATE=3000
LOAD_DURATION=30
LOAD_BATCH=50

run_test() {
  local label="$1"
  local extra_opts="$2"

  echo ""
  echo "════════════════════════════════════════"
  echo "TEST: $label"
  echo "════════════════════════════════════════"

  echo "Restarting cluster with: $extra_opts"
  SOLR_EXTRA_OPTS="$extra_opts" docker compose up -d --force-recreate solr1 solr2 solr3

  echo "Waiting for cluster to come up..."
  sleep 15
  until curl -sf "$SOLR/solr/admin/collections?action=CLUSTERSTATUS" >/dev/null 2>&1; do
    sleep 3
  done
  echo "Cluster up."

  echo "Running load: $LOAD_RATE doc/s for ${LOAD_DURATION}s..."
  python3 load.py --rate "$LOAD_RATE" --duration "$LOAD_DURATION" \
    --batch "$LOAD_BATCH" --threads 2 &
  LOAD_PID=$!

  sleep 10  # wait for connections to stabilize

  echo ""
  echo "TCP connections per Solr port (mid-load):"
  for port in 8981 8982 8983; do
    count=$(ss -tn state established 2>/dev/null | grep -c ":$port" || echo 0)
    printf "  port %d: %4d established\n" "$port" "$count"
  done

  echo ""
  echo "solr.update.client.connections metrics:"
  curl -sf "$SOLR/solr/admin/metrics?prefix=solr.update.client.connections&wt=json" | \
    python3 -c "
import sys, json
d = json.load(sys.stdin)
for k, v in sorted(d.get('metrics', {}).items()):
    if 'update.client.connections' in k:
        print(f'  {k} = {v}')
" 2>/dev/null

  wait "$LOAD_PID" 2>/dev/null || true
}

# Test 1: HTTP/1.1 (прогнозируем много соединений)
run_test "HTTP/1.1 (solr.http1=true)" "-Dsolr.http1=true"

# Test 2: HTTP/2 default (прогнозируем ~4 соединения на пару нод)
run_test "HTTP/2 (default, solr.http1 not set)" ""

# Test 3: HTTP/2 с maxConnectionsPerDestination=2 (минимум)
run_test "HTTP/2 maxConnPerDest=2" "-Dsolr.http2.maxConnectionsPerDestination=2"

echo ""
echo "════════════════════════════════════════"
echo "DONE. Restore normal mode:"
echo "  docker compose up -d"
echo "════════════════════════════════════════"
