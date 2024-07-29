#!/usr/bin/env bash

set -xe

PLUGIN_NAME="startree-pinot-datasource"
PLUGIN_SRC="dist"
PLUGIN_VER="1.0.1"
PLUGIN_ARCHIVE="${PLUGIN_NAME}-${PLUGIN_VER}.zip"

K8S_NAMESPACE="cell-9itmgf-default"
K8S_POD="pinot-grafana-demo-0"
CONTAINER_PLUGINS_PATH="/var/lib/grafana/plugins"

build() {
  npm run build
  mage -v
  npx @grafana/sign-plugin@latest --rootUrls https://pinot-grafana-demo.9itmgf.cp.s7e.startree.cloud,https://grafana.gcstest.graphiant.startree.cloud,https://grafana.gcsproduction.graphiant.startree.cloud,https://grafana.la32x9.cp.s7e.startree-dev.cloud

  mv "${PLUGIN_SRC}" "${PLUGIN_NAME}"
  zip -r ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_NAME}"
  mv ".${PLUGIN_ARCHIVE}-next" "${PLUGIN_ARCHIVE}"
  mv "${PLUGIN_NAME}" "${PLUGIN_SRC}"
}

deploy() {
  # Deploy for demo cluster
  kubectl --namespace "${K8S_NAMESPACE}" cp "${PLUGIN_ARCHIVE}" "${K8S_POD}":"${CONTAINER_PLUGINS_PATH}"
  kubectl --namespace "${K8S_NAMESPACE}" exec "${K8S_POD}" -- rm -rf "${CONTAINER_PLUGINS_PATH}/${PLUGIN_NAME}"
  kubectl --namespace "${K8S_NAMESPACE}" exec "${K8S_POD}" -- unzip "${CONTAINER_PLUGINS_PATH}/${PLUGIN_ARCHIVE}" -d "${CONTAINER_PLUGINS_PATH}"
  kubectl --namespace "${K8S_NAMESPACE}" delete pod "${K8S_POD}"
}

build
deploy
