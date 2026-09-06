#!/usr/bin/env bash
# Builds a Lambda layer zip containing pandas + pyarrow, compiled for the
# Lambda execution environment (Amazon Linux 2023, python3.12), regardless
# of what OS you're running this script on.
#
# Usage: ./build_layer.sh
# Output: ./build/layer.zip  (consumed by terraform/modules/lambda/main.tf)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
LAYER_DIR="${BUILD_DIR}/python"

rm -rf "${BUILD_DIR}"
mkdir -p "${LAYER_DIR}"

docker run --rm \
  -v "${SCRIPT_DIR}:/var/task" \
  public.ecr.aws/sam/build-python3.12 \
  pip install -r /var/task/requirements.txt -t /var/task/build/python

cd "${BUILD_DIR}"
zip -r layer.zip python >/dev/null
rm -rf python

echo "Built ${BUILD_DIR}/layer.zip"
