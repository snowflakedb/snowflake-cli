#!/usr/bin/env bash
# Local wrapper: bulk-migrate all release-v* branches.
# Discovers release-v* branches in the source repo, computes --last-rev for each
# (parent of the branch tip), and feeds them into migrate-branch.sh one by one.
# Tokens are minted once and the Docker image is built once for the whole run.
#
# Usage: local-migrate-all-release-branches.sh [--dry-run]
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

# ---------- prerequisite checks ----------
command -v gh     >/dev/null 2>&1 || { echo "ERROR: gh CLI not found" >&2;     exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: docker not found" >&2;     exit 1; }
[[ -f "${SCRIPT_DIR}/.env" ]]     || { echo "ERROR: ${SCRIPT_DIR}/.env not found (required by local-token-minter.sh)" >&2; exit 1; }

# ---------- argument parsing ----------
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ---------- config (needed for SOURCE_ORG, COPYBARA_RELEASE, etc.) ----------
# shellcheck source=../lib/config.sh
source "${SCRIPT_DIR}/../lib/config.sh"

# ---------- token minting (once for all branches) ----------
echo "Generating JWT..."
echo "Exchanging JWT for Installation Access Token via GitHub CLI..."
export SOURCE_GH_TOKEN
SOURCE_GH_TOKEN=$(gh auth token)
export MIRROR_GH_TOKEN
MIRROR_GH_TOKEN=$(bash "${SCRIPT_DIR}/local-token-minter.sh")

# ---------- Docker image (built once) ----------
echo "Building Copybara Docker image..."
docker build \
  --build-arg "COPYBARA_RELEASE=${COPYBARA_RELEASE}" \
  -t copybara:local \
  -f "${SCRIPT_DIR}/../copybara/Dockerfile" \
  "${SCRIPT_DIR}/../copybara"

# ---------- branch discovery ----------
echo "Listing release branches from ${SOURCE_ORG}/${SOURCE_REPO}..."

branches=$(gh api "repos/${SOURCE_ORG}/${SOURCE_REPO}/branches" \
  --paginate \
  --jq '.[].name' \
  | grep '^release-v' \
  | sort)

if [[ -z "${branches}" ]]; then
  echo "No release-v* branches found."
  exit 0
fi

echo ""
echo "Found release branches:"
echo "${branches}" | sed 's/^/  /'
echo ""

# ---------- migrate each branch ----------
extra_args=()
[[ "${DRY_RUN}" == true ]] && extra_args+=("--dry-run")

failed=()

while IFS= read -r branch; do
  last_rev=$(gh api \
    "repos/${SOURCE_ORG}/${SOURCE_REPO}/commits?sha=${branch}&per_page=2" \
    --jq '.[1].sha // empty')

  if [[ -z "${last_rev}" ]]; then
    echo "WARNING: ${branch} has only one commit — skipping (cannot compute parent SHA)" >&2
    continue
  fi

  echo "--- Migrating ${branch} (--last-rev ${last_rev}) ---"
  if ! "${SCRIPT_DIR}/../migrate-branch.sh" \
       --branch "${branch}" \
       --last-rev "${last_rev}" \
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

echo "All release branches migrated successfully."
