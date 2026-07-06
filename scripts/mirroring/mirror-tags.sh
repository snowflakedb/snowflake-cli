#!/usr/bin/env bash
# Mirror v* tags from source to public mirror.
#
# Fetches all tags from the source repo and mirrors those matching the release
# or RC patterns. Tags not matching any known pattern are skipped with a log
# line — custom tag support is deferred (see adr/0002-tag-mirroring-strategy.md).
#
# For each matched tag:
#   - Skips tags already present on the mirror (idempotent).
#   - Resolves the source commit SHA (dereferencing annotated tags).
#   - Infers the expected mirror branch from the tag name and searches it plus
#     main. Fails loudly if not found on either — likely a tagging mistake.
#   - Recreates annotated tags under Mirror Bot identity (message preserved).
#
# Uses gh api exclusively — no git-over-HTTPS (avoids libcurl/Nix issues).
# Failures are collected and reported at the end (non-zero exit if any failed).
#
# Usage: mirror-tags.sh [--dry-run]
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

branch_exists_on_mirror() {
  local branch="$1"
  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/branches/${branch}" \
    >/dev/null 2>&1
}

# search_branch <branch> <src_commit_sha>
# Paginates commits on <branch> looking for GitOrigin-RevId: <src_commit_sha>.
# Prints the destination commit SHA on success; prints nothing on failure.
search_branch() {
  local branch="$1"
  local src_sha="$2"
  local marker="GitOrigin-RevId: ${src_sha}"

  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/commits?sha=${branch}&per_page=100" \
    --paginate \
    --jq ".[] | select(.commit.message | contains(\"${marker}\")) | .sha" \
    2>/dev/null | head -1 || true
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
skipped=0
mirrored=0

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
    skipped=$((skipped + 1))
    echo ""
    continue
  fi

  # Skip if already on mirror.
  if GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
       "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs/tags/${tag}" \
       >/dev/null 2>&1; then
    echo "ℹ Skipped ${tag} (already on mirror)"
    skipped=$((skipped + 1))
    echo ""
    continue
  fi

  # Resolve source commit SHA, detecting annotated vs lightweight.
  { read -r ref_type; read -r ref_sha; } < <(GH_TOKEN="${SOURCE_GH_TOKEN}" gh api \
    "repos/${SOURCE_ORG}/${SOURCE_REPO}/git/refs/tags/${tag}" \
    --jq '.object.type, .object.sha')

  tag_message=""
  if [[ "${ref_type}" == "tag" ]]; then
    # Annotated — dereference tag object to get commit SHA and message.
    { read -r src_commit_sha; tag_message=$(cat); } < <(GH_TOKEN="${SOURCE_GH_TOKEN}" gh api \
      "repos/${SOURCE_ORG}/${SOURCE_REPO}/git/tags/${ref_sha}" \
      --jq '.object.sha, .message')
  else
    # Lightweight — ref points directly to the commit.
    src_commit_sha="${ref_sha}"
  fi

  # Determine the search order based on tag type.
  release_branch=$(infer_release_branch "${tag}")
  if [[ "${tag_type}" == "release" ]]; then
    search_order=("${release_branch}" "main")
  else
    search_order=("main" "${release_branch}")
  fi

  # Search candidate branches for the GitOrigin-RevId marker.
  dst_commit_sha=""
  for branch in "${search_order[@]}"; do
    if ! branch_exists_on_mirror "${branch}"; then
      continue
    fi
    dst_commit_sha=$(search_branch "${branch}" "${src_commit_sha}")
    [[ -n "${dst_commit_sha}" ]] && break
  done

  if [[ -z "${dst_commit_sha}" ]]; then
    echo "✗ ${tag} — not found on ${search_order[*]}; check tagging on source" >&2
    failed+=("${tag}")
    echo ""
    continue
  fi

  if [[ "${DRY_RUN}" == true ]]; then
    echo "[dry-run] Would mirror ${tag} (${ref_type}) → ${dst_commit_sha}"
    mirrored=$((mirrored + 1))
    echo ""
    continue
  fi

  # Create the tag on the mirror.
  if [[ "${ref_type}" == "tag" ]]; then
    dst_tag_obj_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
      "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/tags" \
      -f "tag=${tag}" \
      -f "message=${tag_message}" \
      -f "object=${dst_commit_sha}" \
      -f "type=commit" \
      -F "tagger[name]=${MIRROR_BOT_NAME}" \
      -F "tagger[email]=${MIRROR_BOT_EMAIL}" \
      --jq '.sha')
    GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
      "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs" \
      -f "ref=refs/tags/${tag}" \
      -f "sha=${dst_tag_obj_sha}" \
      >/dev/null
    echo "✓ Mirrored ${tag} (annotated)"
  else
    GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
      "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs" \
      -f "ref=refs/tags/${tag}" \
      -f "sha=${dst_commit_sha}" \
      >/dev/null
    echo "✓ Mirrored ${tag} (lightweight)"
  fi

  mirrored=$((mirrored + 1))
  echo ""
done <<< "${source_tags}"

# ---------- summary ----------
echo "Summary: ${mirrored} mirrored, ${skipped} skipped, ${#failed[@]} failed."

if [[ ${#failed[@]} -gt 0 ]]; then
  echo "ERROR: the following tags failed:" >&2
  printf '  %s\n' "${failed[@]}" >&2
  exit 1
fi
