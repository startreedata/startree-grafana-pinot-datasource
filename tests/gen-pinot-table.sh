#!/usr/bin/env bash
#
# Runs INSIDE the startree-pinot container. Generates one website fixture table
# with pinot-tools' synthetic data generator and loads it into the local
# cluster. Driven by tests/setup-pinot-tables.sh.
#
# Args: <srcCamelCase> <tbl_snake_case> <numRecords>
#   e.g. gen-pinot-table.sh complexWebsite complex_website 10000
set -euo pipefail

src="$1"; tbl="$2"; n="${3:-10000}"
work="/tmp/${tbl}"
rm -rf "$work"; mkdir -p "$work/data" "$work/seg"; cd "$work"

cp "/home/pinot/generator/${src}_schema.json"    schema.json
cp "/home/pinot/generator/${src}_generator.json" gen.json
cp "/home/pinot/generator/${src}_config.json"    config.json

# The bundled fixtures are camelCase (complexWebsite); the E2E specs reference
# snake_case (complex_website).
sed -i "s/${src}/${tbl}/g" schema.json config.json

# Match the value domains the E2E specs expect (the generator's defaults differ
# from the old remote fixture): the 5 browsers the specs assert, and uppercase
# country codes. No-op for simpleWebsite, which has no dimension columns.
python3 - <<'PY'
import json
g = json.load(open("gen.json"))
for c in g:
    pat = c.get("pattern", {})
    if c.get("column") == "browser":
        pat["values"] = ["chrome", "edge", "firefox", "ie", "safari"]
    if c.get("column") == "country":
        pat["values"] = ["US", "CN", "IN"]
json.dump(g, open("gen.json", "w"))
PY

A=/home/pinot/bin/pinot-admin.sh
$A GenerateData -numRecords "$n" -numFiles 1 -format csv \
  -schemaFile schema.json -schemaAnnotationFile gen.json -outDir "$work/data" -overwrite

# The generator anchors hoursSinceEpoch to "now" (it ignores the SEQUENCE start),
# but the E2E specs query a fixed window 2023-01-01 .. 2025-01-01 (hours
# 464592 .. 482148). Shift the column so its minimum lands on 464592, keeping the
# data inside that window regardless of when CI runs.
for csv in "$work"/data/*.csv; do
  awk -F, '
    NR==1 { for (i=1;i<=NF;i++) if ($i=="hoursSinceEpoch") col=i; print; next }
    { rows[NR]=$0; if (min=="" || $col<min) min=$col }
    END {
      shift = min - 464592
      for (r=2; r<=NR; r++) {
        m=split(rows[r],f,","); f[col]=f[col]-shift
        out=f[1]; for (i=2;i<=m;i++) out=out","f[i]; print out
      }
    }' "$csv" > "$csv.tmp" && mv "$csv.tmp" "$csv"
done

$A CreateSegment -tableConfigFile config.json -schemaFile schema.json \
  -format CSV -dataDir "$work/data" -outDir "$work/seg" -overwrite
$A AddTable -tableConfigFile config.json -schemaFile schema.json -exec
$A UploadSegment -tableName "$tbl" -segmentDir "$work/seg"
