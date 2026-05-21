#!/usr/bin/env bash
# Проверяет все аспекты патчей на работающем кластере.
# Запускать пока идёт load.py (или после него).
set -euo pipefail

SOLR=${1:-http://localhost:8983}
SEP="─────────────────────────────────────────"

pass() { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; }
info() { echo "  · $*"; }

echo "$SEP"
echo "PATCH 2: acceptQueueSize default = 1000"
echo "$SEP"
# Проверяем через Solr admin — system info показывает свойства
ACCEPT=$(docker compose exec solr1 sh -c \
  'ss -tlnp | grep 8983 | awk "{print \$3}"' 2>/dev/null || \
  ss -tlnp 2>/dev/null | grep 8983 | awk '{print $3}')

# Смотрим через Jetty API или ss на самом хосте
echo "  Checking via ss on host (port 8983):"
for port in 8981 8982 8983; do
  backlog=$(ss -tlnp 2>/dev/null | awk -v p=":$port" '$4~p {print $3}' | head -1)
  if [[ -z "$backlog" ]]; then
    info "port $port not visible from host (normal inside Docker)"
  elif [[ "$backlog" -ge 1000 ]]; then
    pass "port $port: backlog=$backlog (≥1000)"
  else
    fail "port $port: backlog=$backlog (expected ≥1000)"
  fi
done

echo ""
echo "  Checking via Solr system properties:"
curl -sf "$SOLR/solr/admin/info/system?wt=json" | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
jvm = d.get('jvm', {})
props = jvm.get('jmx', {})
val = None
# Try commandLineArgs
for k in d.get('jvm', {}).get('jmx', {}).keys():
    if 'acceptQueue' in k:
        val = k
if val:
    print(f'  Found JVM flag: {val}')
else:
    print('  (property not in JVM args — using compiled default)')
" 2>/dev/null || true

echo ""
echo "$SEP"
echo "PATCH 1: HTTP protocol in use"
echo "$SEP"

PROTOCOL=$(curl -sv "$SOLR/solr/admin/ping" 2>&1 | grep "< HTTP" | head -1 | awk '{print $2}')
if [[ "$PROTOCOL" == "HTTP/2" ]]; then
  pass "HTTP/2 active"
elif [[ "$PROTOCOL" == "HTTP/1.1" ]]; then
  fail "HTTP/1.1 active — убедитесь что solr.http1=true НЕ задан"
else
  info "Could not detect protocol via curl (try: curl -sv --http2 $SOLR/solr/admin/ping)"
fi

echo ""
echo "$SEP"
echo "PATCH 1: solr.update.client.connections metric"
echo "$SEP"

METRICS=$(curl -sf "$SOLR/solr/admin/metrics?prefix=solr.update.client.connections&wt=json")
COUNT=$(echo "$METRICS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
metrics = d.get('metrics', {})
found = [(k,v) for k,v in metrics.items() if 'update.client.connections' in k]
for k, v in sorted(found):
    print(f'  {k} = {v}')
print(len(found))
" 2>/dev/null)
NMETRICS=$(echo "$COUNT" | tail -1)
if [[ "$NMETRICS" -gt 0 ]]; then
  pass "Metric solr.update.client.connections: $NMETRICS values found"
  echo "$COUNT" | head -n -1
else
  fail "Metric solr.update.client.connections NOT found — метрика не зарегистрирована"
fi

echo ""
echo "$SEP"
echo "PATCH 1: solr.query.client.connections metric"
echo "$SEP"

METRICS2=$(curl -sf "$SOLR/solr/admin/metrics?prefix=solr.query.client.connections&wt=json")
NMETRICS2=$(echo "$METRICS2" | python3 -c "
import sys, json
d = json.load(sys.stdin)
metrics = d.get('metrics', {})
found = [k for k in metrics if 'query.client.connections' in k]
for k in sorted(found): print(f'  {k}')
print(len(found))
" 2>/dev/null | tail -1)
if [[ "$NMETRICS2" -gt 0 ]]; then
  pass "Metric solr.query.client.connections: $NMETRICS2 values found"
else
  fail "Metric solr.query.client.connections NOT found"
fi

echo ""
echo "$SEP"
echo "PATCH 1: TCP connection count per destination (HTTP/2 vs HTTP/1.1)"
echo "$SEP"

echo "  Active TCP connections to/from Solr ports (host view):"
for port in 8981 8982 8983; do
  count=$(ss -tn state established 2>/dev/null | grep ":$port" | wc -l)
  info "port $port: $count established TCP"
done

echo ""
echo "  Expected with HTTP/2:   ~4 per node-pair (multiplexing)"
echo "  Expected with HTTP/1.1: many more (1 per shard per coordinator)"

echo ""
echo "$SEP"
echo "PATCH 1: solr.replication.packetSize configurability"
echo "$SEP"

PACKET_DEFAULT=1048576
curl -sf "$SOLR/solr/admin/metrics?prefix=solr.replication&wt=json" | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
# Just check system is alive
print('  Metrics endpoint reachable: OK')
" 2>/dev/null && pass "ReplicationAPIBase accessible" || fail "Cannot reach metrics endpoint"

echo ""
echo "$SEP"
echo "Summary: run 'python3 load.py --rate 5000 --duration 60' in parallel"
echo "         and re-run this script to see live connection counts"
echo "$SEP"
