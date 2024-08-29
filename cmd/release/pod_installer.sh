#!/bin/sh

# Run `go run cmd/release/main.go pod_installer` to upload this file to artifactory.

set -xe

mkdir -p ${WORK_DIR}
cd "${WORK_DIR}"

echo "Downloading plugin..."
local_archive="startree-pinot-datasource.zip"
response_headers=$(curl -XGET "${ARCHIVE_SRC}" \
  --silent \
  --location \
  --dump-header /dev/stdout \
  --header "X-JFrog-Art-Api: ${ARTIFACTORY_TOKEN}" \
  --output "${local_archive}")
echo "Download complete."

echo "Checking file integrity..."
checksum=$(echo "${response_headers}" | awk '/^x-checksum-sha256: / { print $2 }')
echo "${checksum}  ${local_archive}" >checksums.txt
if ! echo "${checksum}  ${local_archive}" | sha256sum -c; then
  echo "Downloaded archive has invalid checksum."
  exit 1
fi

echo "Installing plugin..."
mkdir -p "${INSTALL_DIR}"
unzip "${local_archive}" -d "${INSTALL_DIR}"
echo "Installation complete."
