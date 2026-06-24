#!/usr/bin/env bash
#
# Create the E2E fixture tables (complex_website, simple_website) on the local
# Pinot quickstart container and load generated data into them.
#
# Follows pinot-tools' synthetic data generator
# (https://github.com/apache/pinot/blob/master/pinot-tools/src/main/resources/generator/README.md).
# The generator schema/config/annotation files ship inside the StarTree Pinot
# image at /home/pinot/generator, so nothing extra needs to be committed here.
# The actual generate/segment/add/upload steps run inside the container via
# tests/gen-pinot-table.sh.
#
# Usage: tests/setup-pinot-tables.sh [container_name]   (default: pinot)
set -euo pipefail

CONTAINER="${1:-pinot}"
NUM_RECORDS="${PINOT_GEN_NUM_RECORDS:-10000}"
HERE="$(cd "$(dirname "$0")" && pwd)"

docker cp "$HERE/gen-pinot-table.sh" "$CONTAINER:/tmp/gen-pinot-table.sh"

echo ">>> Generating and loading complex_website"
docker exec "$CONTAINER" bash /tmp/gen-pinot-table.sh complexWebsite complex_website "$NUM_RECORDS"
echo ">>> Generating and loading simple_website"
docker exec "$CONTAINER" bash /tmp/gen-pinot-table.sh simpleWebsite simple_website "$NUM_RECORDS"

# Wait until both tables are queryable (segments loaded on the server).
echo ">>> Waiting for fixture tables to become queryable"
for tbl in complex_website simple_website; do
  for i in $(seq 1 30); do
    n=$(curl -s -X POST http://localhost:8000/query/sql -H 'Content-Type: application/json' \
      -d "{\"sql\":\"SELECT COUNT(*) FROM ${tbl}\"}" 2>/dev/null \
      | grep -oE '"rows":\[\[[0-9]+' | grep -oE '[0-9]+$' || true)
    if [ -n "${n:-}" ] && [ "$n" -ge 1 ] 2>/dev/null; then
      echo "    ${tbl}: ${n} rows"; break
    fi
    if [ "$i" -eq 30 ]; then echo "ERROR: ${tbl} never became queryable"; exit 1; fi
    sleep 3
  done
done
echo ">>> Fixture tables ready"
