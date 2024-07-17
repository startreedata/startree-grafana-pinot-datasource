#!/usr/bin/env bash

set -xe

export GRAFANA_ACCESS_POLICY_TOKEN="glc_eyJvIjoiMTA4NzYxNCIsIm4iOiJwbHVnaW4tc2lnbmluZy1zdGFydHJlZS1wbHVnaW4tc2lnbmluZy10b2tlbi1zdGFydHJlZSIsImsiOiJTRktiRDZzTDg1VjlZUGVObzk3NDU3M0oiLCJtIjp7InIiOiJ1cyJ9fQ=="

PLUGIN_NAME="startree-pinot-datasource"
PLUGIN_SRC="dist"
PLUGIN_ARCHIVE="${PLUGIN_NAME}.zip"

npm run build
mage -v
npx @grafana/sign-plugin@latest --rootUrls https://pinot-grafana-demo.9itmgf.cp.s7e.startree.cloud

mv "${PLUGIN_SRC}" "${PLUGIN_NAME}"
zip -r ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_NAME}"
mv ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_ARCHIVE}"
mv "${PLUGIN_NAME}" "${PLUGIN_SRC}"

## deployment
kubectl --namespace cell-9itmgf-default cp startree-pinot-datasource.zip pinot-grafana-demo-0:/var/lib/grafana/plugins
kubectl --namespace cell-9itmgf-default exec pinot-grafana-demo-0 -- rm -rf /var/lib/grafana/plugins/startree-pinot-datasource
kubectl --namespace cell-9itmgf-default exec pinot-grafana-demo-0 -- unzip /var/lib/grafana/plugins/startree-pinot-datasource.zip -d /var/lib/grafana/plugins/
kubectl --namespace cell-9itmgf-default delete pod pinot-grafana-demo-0
