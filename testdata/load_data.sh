#!/bin/bash

set -xe

JAVA_HOME=$(/usr/libexec/java_home -v21)
export JAVA_HOME
java -version

TABLE_SCHEMA_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/testdata/table_schema.json"
TABLE_CONFIG_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/testdata/table_config.json"
DATA_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/testdata/data.json"
PINOT_ADMIN_CMD="/Users/jacksonargo/Documents/GitHub/pinot/build/bin/pinot-admin.sh"
CONTROLLER_PORT="9000"

TABLE_NAME="events"

if ! curl --fail --silent -XGET "http://localhost:${CONTROLLER_PORT}/tables/${TABLE_NAME}/schema"; then
  "${PINOT_ADMIN_CMD}" AddTable -schemaFile "${TABLE_SCHEMA_FILE}" -tableConfigFile "${TABLE_CONFIG_FILE}" -controllerHost localhost -controllerPort "${CONTROLLER_PORT}" -exec
fi

curl -X POST -F file=@"${DATA_FILE}" -H "Content-Type: multipart/form-data" \
  "http://localhost:9000/ingestFromFile?tableNameWithType=${TABLE_NAME}_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22json%22%7D"
