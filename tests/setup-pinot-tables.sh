#!/usr/bin/env bash
#
# Create the E2E fixture tables (complex_website, simple_website) on the local
# Pinot quickstart container and load generated data into them.
#
# Follows pinot-tools' synthetic data generator
# (https://github.com/apache/pinot/blob/master/pinot-tools/src/main/resources/generator/README.md).
# The generator schema/config/annotation files ship inside the StarTree Pinot
# image at /home/pinot/generator, so nothing extra needs to be committed here.
#
# The image's canonical tables are camelCase (complexWebsite/simpleWebsite); the
# E2E specs reference snake_case (complex_website/simple_website), so we rename
# the schemaName/tableName before loading.
#
# Usage: tests/setup-pinot-tables.sh [container_name]   (default: pinot)
set -euo pipefail

CONTAINER="${1:-pinot}"
NUM_RECORDS="${PINOT_GEN_NUM_RECORDS:-10000}"

# Generate -> segment -> add table -> upload, all inside the container (the
# pinot-admin tool and generator resources live there). $1 = camelCase source
# name (complexWebsite), $2 = snake_case target table (complex_website).
gen_and_load() {
  local src="$1" tbl="$2"
  echo ">>> Generating and loading ${tbl} (from ${src})"
  docker exec "$CONTAINER" bash -c '
    set -e
    src="'"$src"'"; tbl="'"$tbl"'"; n="'"$NUM_RECORDS"'"
    work="/tmp/${tbl}"
    rm -rf "$work"; mkdir -p "$work/data" "$work/seg"; cd "$work"
    cp "/home/pinot/generator/${src}_schema.json"    schema.json
    cp "/home/pinot/generator/${src}_generator.json" gen.json
    cp "/home/pinot/generator/${src}_config.json"    config.json
    sed -i "s/${src}/${tbl}/g" schema.json config.json
    A=/home/pinot/bin/pinot-admin.sh
    $A GenerateData -numRecords "$n" -numFiles 1 -format csv \
      -schemaFile schema.json -schemaAnnotationFile gen.json -outDir "$work/data" -overwrite
    # The generator anchors hoursSinceEpoch to "now" (it ignores the SEQUENCE start), but the
    # E2E specs query a fixed window (2023-01-01 .. 2025-01-01 == hours 464592 .. 482148). Shift
    # the column so its minimum lands on 464592 — keeps the data inside the window the tests use,
    # independent of when CI runs. (hoursSinceEpoch is column 5 of the generated CSV; resolve by
    # header to be safe.)
    for csv in "$work"/data/*.csv; do
      awk -F, "
        NR==1 { for (i=1;i<=NF;i++) if (\$i==\"hoursSinceEpoch\") col=i; print; next }
        { rows[NR]=\$0; if (min==\"\" || \$col<min) min=\$col }
        END {
          shift=min-464592
          for (r=2;r<=NR;r++) {
            n=split(rows[r],f,\",\"); f[col]=f[col]-shift
            out=f[1]; for (i=2;i<=n;i++) out=out\",\"f[i]; print out
          }
        }" "$csv" > "$csv.tmp" && mv "$csv.tmp" "$csv"
    done
    $A CreateSegment -tableConfigFile config.json -schemaFile schema.json \
      -format CSV -dataDir "$work/data" -outDir "$work/seg" -overwrite
    $A AddTable -tableConfigFile config.json -schemaFile schema.json -exec
    $A UploadSegment -tableName "$tbl" -segmentDir "$work/seg"
  '
}

gen_and_load complexWebsite complex_website
gen_and_load simpleWebsite simple_website

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
