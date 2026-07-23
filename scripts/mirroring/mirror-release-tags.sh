#!/usr/bin/env bash
# Mirror v* tags from source to public mirror.
#
# Fetches all tags from the source repo and mirrors those matching the release
# or RC patterns. Tags not matching any known pattern are skipped with a log
# line — custom tag support is deferred (see adr/0002-tag-mirroring-strategy.md).
#
# For each matched tag, infers the candidate branches from the tag name and
# delegates to mirror-tag.sh. Failures are collected and reported at the end
# (non-zero exit if any failed).
#
# Uses gh api exclusively — no git-over-HTTPS (avoids libcurl/Nix issues).
#
# Usage: mirror-release-tags.sh [--dry-run]
#
# Required env:
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=lib/config.sh
source "${SCRIPT_DIR}/lib/config.sh"

# Tag patterns. Each drives a specific branch-inference search order.
# Release tags:  vX.Y.Z      → search release-vX.Y.Z first, then main
# RC tags:       vX.Y.Z-rcN  → search main first, then release-vX.Y.Z
RELEASE_TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+$'
RC_TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+$'

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

# ---------- helpers ----------

# infer_release_branch <tag_name>
# Extracts vX.Y.Z from vX.Y.Z or vX.Y.Z-rcN and returns "release-vX.Y.Z".
infer_release_branch() {
  local tag="$1"
  # Keep the three numeric components; strip any suffix (e.g. -rcN).
  # v3.20.0     → release-v3.20.0
  # v3.20.0-rc1 → release-v3.20.0
  echo "${tag}" | sed -E 's/^(v[0-9]+\.[0-9]+\.[0-9]+).*/release-\1/'
}

# ---------- tag discovery ----------
echo "Listing tags from ${SOURCE_ORG}/${SOURCE_REPO}..."

source_tags=$(GH_TOKEN="${SOURCE_GH_TOKEN}" gh api \
  "repos/${SOURCE_ORG}/${SOURCE_REPO}/tags" \
  --paginate \
  --jq '.[].name' \
  | sort -V || true)

if [[ -z "${source_tags}" ]]; then
  echo "No tags found."
  exit 0
fi

tag_count=$(echo "${source_tags}" | wc -l | tr -d ' ')
echo "Found ${tag_count} tag(s) total."
echo ""

# ---------- mirror each tag ----------
failed=()
unmatched=0
succeeded=0
skipped=0

while IFS= read -r tag; do
  [[ -z "${tag}" ]] && continue
  echo "--- Tag ${tag} ---"

  # Classify tag — skip immediately if no pattern matches.
  if [[ "${tag}" =~ ${RELEASE_TAG_RE} ]]; then
    tag_type="release"
  elif [[ "${tag}" =~ ${RC_TAG_RE} ]]; then
    tag_type="rc"
  else
    echo "ℹ Skipped ${tag} (no branch-inference pattern matched)"
    unmatched=$((unmatched + 1))
    echo ""
    continue
  fi

  # Determine the search order based on tag type.
  release_branch=$(infer_release_branch "${tag}")
  if [[ "${tag_type}" == "release" ]]; then
    search_order=("${release_branch}" "main")
  else
    search_order=("main" "${release_branch}")
  fi

  # Build full args list and delegate to the single-tag primitive.
  mirror_args=("${tag}")
  for b in "${search_order[@]}"; do
    mirror_args+=(--branch "${b}")
  done
  [[ "${DRY_RUN}" == true ]] && mirror_args+=(--dry-run)

  exit_code=0
  "${SCRIPT_DIR}/mirror-tag.sh" "${mirror_args[@]}" || exit_code=$?
  if   [[ "${exit_code}" -eq 0 ]]; then succeeded=$((succeeded + 1))
  elif [[ "${exit_code}" -eq 2 ]]; then skipped=$((skipped + 1))
  else                                   failed+=("${tag}")
  fi
  echo ""
done <<< "${source_tags}"

# ---------- summary ----------
echo "Summary: ${succeeded} mirrored/updated, ${skipped} skipped (already on mirror), ${unmatched} unmatched, ${#failed[@]} failed."

if [[ ${#failed[@]} -gt 0 ]]; then
  echo "ERROR: the following tags failed:" >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi
