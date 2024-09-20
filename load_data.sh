#!/bin/bash

set -xe

JAVA_HOME=$(/usr/libexec/java_home -v21)
export JAVA_HOME
java -version

TABLE_SCHEMA_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/table_schema.json"
TABLE_CONFIG_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/table_config.json"
JOB_SPEC_FILE="/Users/jacksonargo/startree-grafana-pinot-datasource/upload_job.yaml"
PINOT_ADMIN_CMD="/Users/jacksonargo/Documents/GitHub/pinot/build/bin/pinot-admin.sh"
CONTROLLER_PORT="9000"

TABLE_NAME="events"

if ! curl --silent -XGET "http://localhost:${CONTROLLER_PORT}/tables/${TABLE_NAME}/schema"; then
  "${PINOT_ADMIN_CMD}" AddTable -schemaFile "${TABLE_SCHEMA_FILE}" -tableConfigFile "${TABLE_CONFIG_FILE}" -controllerPort "${CONTROLLER_PORT}" -exec
fi
"${PINOT_ADMIN_CMD}" LaunchDataIngestionJob -jobSpecFile ${JOB_SPEC_FILE}
