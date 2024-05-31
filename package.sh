#!/usr/bin/env bash

set -xe

PLUGIN_NAME="startree-pinot-datasource"
PLUGIN_SRC="dist"
PLUGIN_ARCHIVE="${PLUGIN_NAME}-1.0.0.zip"

npm run build
mage -v
npx @grafana/sign-plugin@latest --rootUrls https://pinot-grafana-demo.9itmgf.cp.s7e.startree.cloud,https://grafana.gcstest.graphiant.startree.cloud,https://grafana.gcsproduction.graphiant.startree.cloud,https://grafana.la32x9.cp.s7e.startree-dev.cloud

mv "${PLUGIN_SRC}" "${PLUGIN_NAME}"
zip -r ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_NAME}"
mv ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_ARCHIVE}"
mv "${PLUGIN_NAME}" "${PLUGIN_SRC}"

## deployment
kubectl --namespace cell-9itmgf-default cp startree-pinot-datasource-1.0.0.zip pinot-grafana-demo-0:/var/lib/grafana/plugins
kubectl --namespace cell-9itmgf-default exec pinot-grafana-demo-0 -- rm -rf /var/lib/grafana/plugins/startree-pinot-datasource
kubectl --namespace cell-9itmgf-default exec pinot-grafana-demo-0 -- unzip /var/lib/grafana/plugins/startree-pinot-datasource-1.0.0.zip -d /var/lib/grafana/plugins/
kubectl --namespace cell-9itmgf-default delete pod pinot-grafana-demo-0
