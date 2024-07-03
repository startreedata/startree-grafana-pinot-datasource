#!/usr/bin/env bash

set -xe

npm run build
mage -v

mv "dist" "startree-pinot-datasource"
zip -r "startree-pinot-datasource.zip" "startree-pinot-datasource"

mv "startree-pinot-datasource" "dist"
