#!/usr/bin/env bash

set -xe

export GRAFANA_ACCESS_POLICY_TOKEN="glc_eyJvIjoiMTE2ODc3NCIsIm4iOiJwbHVnaW4tc2lnbmluZy1zaWduaW5nLXRva2VuIiwiayI6ImZoSU83MmI2M2EzMGZyTDg2dDZtZXkzYiIsIm0iOnsiciI6InVzIn19"

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
