#!/usr/bin/env bash
# Local wrapper for migrate-branch.sh.
# Mints tokens from the local GitHub CLI and App credentials, builds the
# Copybara Docker image, then delegates to the core script.
#
# Usage: local-migrate-branch.sh --branch <name> --last-rev <sha> [--dry-run]
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# ---------- prerequisite checks ----------
command -v gh     >/dev/null 2>&1 || { echo "ERROR: gh CLI not found" >&2;     exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found" >&2;     exit 1; }
[[ -f "${SCRIPT_DIR}/.env" ]]     || { echo "ERROR: ${SCRIPT_DIR}/.env not found (required by local-token-minter.sh)" >&2; exit 1; }

# ---------- token minting ----------
echo "Generating JWT..."
echo "Exchanging JWT for Installation Access Token via GitHub CLI..."
export SOURCE_GH_TOKEN
SOURCE_GH_TOKEN=$(gh auth token)
export MIRROR_GH_TOKEN
MIRROR_GH_TOKEN=$(bash "${SCRIPT_DIR}/local-token-minter.sh")

# ---------- Docker image ----------
# shellcheck source=../lib/config.sh
source "${SCRIPT_DIR}/../lib/config.sh"
echo "Building Copybara Docker image..."
docker build \
  --build-arg "COPYBARA_RELEASE=${COPYBARA_RELEASE}" \
  -t copybara:local \
  -f "${SCRIPT_DIR}/../copybara/Dockerfile" \
  "${SCRIPT_DIR}/../copybara"

# ---------- delegate ----------
exec "${SCRIPT_DIR}/../migrate-branch.sh" "$@"
