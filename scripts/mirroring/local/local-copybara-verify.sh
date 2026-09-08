#!/usr/bin/env bash
# Local wrapper for lib/copybara-verify.sh.
# Builds the Copybara Docker image, then runs verify against HEAD.
# No GitHub tokens required.
#
# Usage: local-copybara-verify.sh
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found" >&2; exit 1; }

# shellcheck source=../lib/config.sh
source "${SCRIPT_DIR}/../lib/config.sh"
echo "Building Copybara Docker image..."
docker build \
  --build-arg "COPYBARA_RELEASE=${COPYBARA_RELEASE}" \
  -t copybara:local \
  -f "${SCRIPT_DIR}/../copybara/Dockerfile" \
  "${SCRIPT_DIR}/../copybara"

exec bash "${SCRIPT_DIR}/../lib/copybara-verify.sh"
