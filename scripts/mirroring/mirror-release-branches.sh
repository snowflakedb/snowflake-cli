#!/usr/bin/env bash
# Incremental mirror of all release-v* branches.
# Discovers release-v* branches in the source repo and runs mirror-branch.sh for each.
#
# Usage: mirror-release-branches.sh [--dry-run]
#
# Required env:
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=lib/config.sh
source "${SCRIPT_DIR}/lib/config.sh"

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

# ---------- branch discovery ----------
echo "Listing release branches from ${SOURCE_ORG}/${SOURCE_REPO}..."

branches=$(gh api \
  "repos/${SOURCE_ORG}/${SOURCE_REPO}/branches" \
  --paginate \
  --jq '.[].name' \
  | grep '^release-v' \
  | sort)

if [[ -z "${branches}" ]]; then
  echo "No release-v* branches found."
  exit 0
fi

echo "Found release branches:"
echo "${branches}" | sed 's/^/  /'
echo ""

# ---------- mirror each branch ----------
extra_args=()
[[ "${DRY_RUN}" == true ]] && extra_args+=("--dry-run")

failed=()

while IFS= read -r branch; do
  echo "--- Mirroring ${branch} ---"
  if ! "${SCRIPT_DIR}/mirror-branch.sh" \
       --branch "${branch}" \
       "${extra_args[@]}"; then
    failed+=("${branch}")
  fi
  echo ""
done <<< "${branches}"

# ---------- summary ----------
if [[ ${#failed[@]} -gt 0 ]]; then
  echo "ERROR: the following branches failed:" >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi

echo "All release branches mirrored successfully."
