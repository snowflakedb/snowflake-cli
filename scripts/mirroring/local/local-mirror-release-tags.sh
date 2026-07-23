#!/usr/bin/env bash
# Local wrapper for mirror-release-tags.sh.
# Mints tokens from the local GitHub CLI and App credentials, then delegates
# to the core script. No Docker required — tag mirroring uses gh api only.
#
# Usage: local-mirror-release-tags.sh [--dry-run]
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# ---------- prerequisite checks ----------
command -v gh >/dev/null 2>&1 || { echo "ERROR: gh CLI not found" >&2; exit 1; }
[[ -f "${SCRIPT_DIR}/.env" ]] || { echo "ERROR: ${SCRIPT_DIR}/.env not found (required by local-token-minter.sh)" >&2; exit 1; }

# ---------- token minting ----------
export SOURCE_GH_TOKEN
SOURCE_GH_TOKEN=$(gh auth token)
export MIRROR_GH_TOKEN
MIRROR_GH_TOKEN=$(bash "${SCRIPT_DIR}/local-token-minter.sh")

# ---------- delegate ----------
exec "${SCRIPT_DIR}/../mirror-release-tags.sh" "$@"
