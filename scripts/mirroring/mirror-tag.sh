#!/usr/bin/env bash
# Mirror a single tag from source to the public mirror.
#
# Resolves the source commit SHA (dereferencing annotated tags), searches the
# given candidate branches for the matching GitOrigin-RevId trailer, and
# creates the tag on the mirror. Recreates annotated tags under Mirror Bot
# identity (message preserved); lightweight tags are created as-is.
#
# Usage: mirror-tag.sh <tag-name> --branch <b1> [--branch <b2> ...] [--dry-run] [--update]
#
# Options:
#   --branch    Candidate branch to search for GitOrigin-RevId (repeatable;
#               searched in the order given; at least one required)
#   --dry-run   Print what would happen without writing to the mirror
#   --update    If the tag already exists on the mirror, resolve the destination
#               SHA and update the ref if it differs. Without this flag, existing
#               tags are skipped.
#
# Exit codes:
#   0   Tag mirrored or updated successfully
#   1   Tag could not be mirrored (not found on any branch, API error, bad args)
#   2   Tag skipped (already on mirror / already up to date)
#
# Required env:
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=lib/config.sh
source "${SCRIPT_DIR}/lib/config.sh"

# ---------- argument parsing ----------
TAG=""
BRANCHES=()
DRY_RUN=false
UPDATE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)
      [[ $# -lt 2 ]] && { echo "ERROR: --branch requires an argument" >&2; exit 1; }
      BRANCHES+=("$2"); shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    --update)  UPDATE=true; shift ;;
    -*)
      echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
    *)
      [[ -n "${TAG}" ]] && { echo "ERROR: unexpected argument: $1" >&2; exit 1; }
      TAG="$1"; shift ;;
  esac
done

# ---------- validation ----------
[[ -z "${TAG}" ]] && { echo "ERROR: tag name is required" >&2; exit 1; }
[[ ${#BRANCHES[@]} -eq 0 ]] && { echo "ERROR: at least one --branch is required" >&2; exit 1; }
[[ -z "${SOURCE_GH_TOKEN:-}" ]] && { echo "ERROR: SOURCE_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${MIRROR_GH_TOKEN:-}" ]] && { echo "ERROR: MIRROR_GH_TOKEN is not set" >&2; exit 1; }

# ---------- helpers ----------

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

# ---------- check if already on mirror ----------
TAG_EXISTS=false
if GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
     "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/ref/tags/${TAG}" \
     >/dev/null 2>&1; then
  TAG_EXISTS=true
  if [[ "${UPDATE}" == false ]]; then
    echo "ℹ Skipped ${TAG} (already on mirror)"
    exit 2
  fi
fi

# ---------- resolve source commit SHA ----------
src_ref_info=$(GH_TOKEN="${SOURCE_GH_TOKEN}" gh api \
  "repos/${SOURCE_ORG}/${SOURCE_REPO}/git/ref/tags/${TAG}" \
  --jq '.object.type, .object.sha' 2>/dev/null) \
  || { echo "✗ ${TAG} — tag not found on source (${SOURCE_ORG}/${SOURCE_REPO})" >&2; exit 1; }
{ read -r ref_type; read -r ref_sha; } <<< "${src_ref_info}"

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

# ---------- fast-path up-to-date check (--update only) ----------
# Avoids the expensive paginated branch walk when the mirror tag already
# points at the mirrored version of src_commit_sha.
if [[ "${TAG_EXISTS}" == true ]]; then
  { read -r _fp_ref_type; read -r _fp_ref_sha; } < <(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/ref/tags/${TAG}" \
    --jq '.object.type, .object.sha')
  if [[ "${_fp_ref_type}" == "tag" ]]; then
    _fp_commit_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
      "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/tags/${_fp_ref_sha}" \
      --jq '.object.sha')
  else
    _fp_commit_sha="${_fp_ref_sha}"
  fi
  _fp_origin_rev=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/commits/${_fp_commit_sha}" \
    --jq '.message' 2>/dev/null \
    | grep -m1 "^GitOrigin-RevId: " | awk '{print $2}' || true)
  if [[ "${_fp_origin_rev}" == "${src_commit_sha}" ]]; then
    echo "ℹ Skipped ${TAG} (already up to date)"
    exit 2
  fi
fi

# ---------- find destination commit SHA ----------
dst_commit_sha=""
for branch in "${BRANCHES[@]}"; do
  if ! branch_exists_on_mirror "${branch}"; then
    continue
  fi
  dst_commit_sha=$(search_branch "${branch}" "${src_commit_sha}")
  [[ -n "${dst_commit_sha}" ]] && break
done

if [[ -z "${dst_commit_sha}" ]]; then
  cat >&2 <<EOF
✗ ${TAG} — cannot locate mirrored commit on the mirror

  Source commit : ${src_commit_sha}  (resolved from tag ${TAG})
  Searched      : ${BRANCHES[*]} (mirror)

  Every commit Copybara mirrors carries a 'GitOrigin-RevId: <sha>'
  trailer. None of the commits on the searched branch(es) carry
  'GitOrigin-RevId: ${src_commit_sha}', so the destination SHA cannot be
  determined.

  Likely causes:
    * The source commit has not been mirrored to this branch yet —
      run mirror-branch.sh for '${BRANCHES[*]}' first.
    * The commit lives on a different branch on the mirror —
      retry with the correct --branch.
    * The commit predates the Copybara baseline and is part of the
      shared history — it exists on the mirror under the same SHA
      but was never annotated with a trailer.
EOF
  exit 1
fi

# ---------- dry-run ----------
if [[ "${DRY_RUN}" == true ]]; then
  if [[ "${TAG_EXISTS}" == true ]]; then
    echo "[dry-run] Would update ${TAG} (${ref_type}) → ${dst_commit_sha}"
  else
    echo "[dry-run] Would mirror ${TAG} (${ref_type}) → ${dst_commit_sha}"
  fi
  exit 0
fi

# ---------- create or update tag on mirror ----------
if [[ "${ref_type}" == "tag" ]]; then
  write_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/tags" \
    -f "tag=${TAG}" \
    -f "message=${tag_message}" \
    -f "object=${dst_commit_sha}" \
    -f "type=commit" \
    -F "tagger[name]=${MIRROR_BOT_NAME}" \
    -F "tagger[email]=${MIRROR_BOT_EMAIL}" \
    --jq '.sha')
  tag_kind="annotated"
else
  write_sha="${dst_commit_sha}"
  tag_kind="lightweight"
fi

if [[ "${TAG_EXISTS}" == false ]]; then
  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs" \
    -f "ref=refs/tags/${TAG}" \
    -f "sha=${write_sha}" \
    >/dev/null
  echo "✓ Mirrored ${TAG} (${tag_kind})"
else
  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs/tags/${TAG}" \
    -X PATCH \
    -f "sha=${write_sha}" \
    -F "force=true" \
    >/dev/null
  echo "✓ Updated ${TAG} (${tag_kind})"
fi
