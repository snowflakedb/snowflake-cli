#!/usr/bin/env bash
# Scheduled incremental mirror of the main branch.
# No branching logic — if the destination has no GitOrigin-RevId baseline,
# Copybara fails fast. Run migrate-branch.sh --branch main first.
#
# Usage: mirror-main.sh [--dry-run]
#
# Required env:
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=lib/copybara-run.sh
source "${SCRIPT_DIR}/lib/copybara-run.sh"

# ---------- argument parsing ----------
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ---------- validation ----------
[[ -z "${SOURCE_GH_TOKEN:-}" ]] && { echo "ERROR: SOURCE_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${MIRROR_GH_TOKEN:-}" ]] && { echo "ERROR: MIRROR_GH_TOKEN is not set" >&2; exit 1; }

# ---------- run ----------
extra_args=()
[[ "${DRY_RUN}" == true ]] && extra_args+=("--dry-run")

rc=0
copybara_run "${COPYBARA_WORKFLOW}" "${extra_args[@]}" || rc=$?
handle_copybara_rc "${rc}" "main"
