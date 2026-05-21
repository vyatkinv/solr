#!/usr/bin/env bash
# Создаёт тестовую коллекцию: 3 шарда, RF=2 (6 реплик на 3 нодах)
set -euo pipefail

SOLR=http://localhost:8983
COLLECTION=test_write

wait_solr() {
  echo "Waiting for Solr cluster..."
  until curl -sf "$SOLR/solr/admin/collections?action=CLUSTERSTATUS" | \
      python3 -c "import sys,json; d=json.load(sys.stdin); sys.exit(0 if len(d['cluster']['live_nodes'])>=3 else 1)" 2>/dev/null; do
    sleep 2
  done
  echo "All 3 nodes live."
}

wait_solr

echo "Creating collection '$COLLECTION' (3 shards, RF=2)..."
curl -sf "$SOLR/solr/admin/collections" \
  --data-urlencode "action=CREATE" \
  --data-urlencode "name=$COLLECTION" \
  --data-urlencode "numShards=3" \
  --data-urlencode "replicationFactor=2" \
  --data-urlencode "maxShardsPerNode=6" | python3 -m json.tool

echo ""
echo "Collection status:"
curl -sf "$SOLR/solr/admin/collections?action=CLUSTERSTATUS&collection=$COLLECTION" | \
  python3 -c "
import sys, json
d = json.load(sys.stdin)
coll = d['cluster']['collections']['$COLLECTION']
for shard, sdata in coll['shards'].items():
    for replica, rdata in sdata['replicas'].items():
        print(f'  {shard} {replica}: {rdata[\"node_name\"]} [{rdata[\"state\"]}] {rdata[\"type\"]}')
"
