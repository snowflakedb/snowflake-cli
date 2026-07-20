#!/usr/bin/env bash
# One-time bootstrap: establish GitOrigin-RevId baseline on a branch that has
# never been mirrored. Run manually by a maintainer before the first scheduled
# mirror run for that branch.
#
# Uses ITERATIVE mode to replay individual commits since --last-rev. If
# ITERATIVE produces NO_OP (branch was seeded via git push --mirror so source
# == destination tree), creates a single baseline commit via the GitHub API.
#
# Usage: migrate-branch.sh --branch <name> --last-rev <sha> [--dry-run]
#
# Required env:
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=lib/copybara-run.sh
source "${SCRIPT_DIR}/lib/copybara-run.sh"

# ---------- argument parsing ----------
BRANCH=""
LAST_REV=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)   BRANCH="${2:?ERROR: --branch requires a value}";    shift 2 ;;
    --last-rev) LAST_REV="${2:?ERROR: --last-rev requires a value}"; shift 2 ;;
    --dry-run)  DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ---------- validation ----------
[[ -z "${SOURCE_GH_TOKEN:-}" ]] && { echo "ERROR: SOURCE_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${MIRROR_GH_TOKEN:-}" ]] && { echo "ERROR: MIRROR_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${BRANCH}"            ]] && { echo "ERROR: --branch is required" >&2;       exit 1; }
[[ -z "${LAST_REV}"          ]] && { echo "ERROR: --last-rev is required" >&2;     exit 1; }
[[ ! "${LAST_REV}" =~ ^[0-9a-f]{40}$ ]] \
  && { echo "ERROR: --last-rev must be a 40-char lowercase hex SHA, got: ${LAST_REV}" >&2; exit 1; }

# ---------- idempotency guard ----------
# Abort if the destination branch tip already carries a GitOrigin-RevId trailer —
# the baseline is established and mirror-branch.sh should handle ongoing sync.
# Uses MIRROR_GH_TOKEN explicitly so gh targets the mirror org regardless of
# whatever credentials are active in the local gh credential store.
existing_tip_msg=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/branches/${BRANCH}" \
    --jq '.commit.commit.message' 2>/dev/null) || true
# (empty = branch not yet on mirror, or API call failed — proceed in both cases)

if [[ "${existing_tip_msg}" == *"GitOrigin-RevId:"* ]]; then
  echo "ERROR: '${BRANCH}' already has a GitOrigin-RevId on its mirror tip." >&2
  echo "       Baseline is established — use mirror-branch.sh for ongoing sync." >&2
  exit 1
fi

# ---------- build Copybara args ----------
extra_args=()
[[ "${DRY_RUN}" == true ]] && extra_args+=("--dry-run")

# main uses only the workflow name; other branches require a positional source
# ref and --git-destination-push to override the "main" defaults in copy.bara.sky.
if [[ "${BRANCH}" == "main" ]]; then
  iterative_args=("${COPYBARA_WORKFLOW}" "--last-rev=${LAST_REV}"                                                 "${extra_args[@]}")
else
  iterative_args=("${COPYBARA_WORKFLOW}" "refs/heads/${BRANCH}" "--git-destination-push=refs/heads/${BRANCH}" "--last-rev=${LAST_REV}" "${extra_args[@]}")
fi

# ---------- helpers ----------
# Push a single baseline commit to the mirror branch: adds an empty .copybara
# file (creating a real tree diff) and embeds a GitOrigin-RevId trailer pointing
# to the current source branch tip.
#
# Copybara detects GitOrigin-RevId only on commits with a non-empty tree diff;
# empty commits and same-content commits are skipped during trailer scanning.
# The .copybara file provides that diff. On the first subsequent mirror run,
# Copybara replaces the full origin_files tree with the source, which drops
# .copybara automatically since it is absent from the source.
create_baseline_commit() {
  local branch="$1"
  echo "ℹ Creating baseline commit (.copybara + GitOrigin-RevId) on ${MIRROR_ORG}/${MIRROR_REPO}@${branch}..."

  local source_tip
  source_tip=$(gh api \
    "repos/${SOURCE_ORG}/${SOURCE_REPO}/branches/${branch}" \
    --jq '.commit.sha')

  local branch_json parent_sha tree_sha
  branch_json=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/branches/${branch}")
  parent_sha=$(printf '%s' "${branch_json}" | jq -r '.commit.sha')
  tree_sha=$(printf '%s' "${branch_json}" | jq -r '.commit.commit.tree.sha')

  local blob_sha
  blob_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/blobs" \
    -f content="" -f encoding="utf-8" \
    --jq '.sha')

  local new_tree_sha
  new_tree_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/trees" \
    --input <(jq -n \
      --arg base_tree "${tree_sha}" \
      --arg sha       "${blob_sha}" \
      '{base_tree: $base_tree, tree: [{path: ".copybara", mode: "100644", type: "blob", sha: $sha}]}') \
    --jq '.sha')

  local commit_message
  commit_message=$(printf 'Copybara import baseline\n\nGitOrigin-RevId: %s' "${source_tip}")

  local new_commit_sha
  new_commit_sha=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/commits" \
    --input <(jq -n \
      --arg message "${commit_message}" \
      --arg tree   "${new_tree_sha}" \
      --arg parent "${parent_sha}" \
      '{message: $message, tree: $tree, parents: [$parent]}') \
    --jq '.sha')

  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs/heads/${branch}" \
    -X PATCH -f "sha=${new_commit_sha}" >/dev/null

  echo "  Baseline commit: ${new_commit_sha} (GitOrigin-RevId: ${source_tip})"
}

# ---------- run ----------
rc=0
copybara_run "${iterative_args[@]}" || rc=$?

if [[ "${rc}" -eq 4 ]]; then
  # NO_OP: branch was seeded via git push --mirror so source == destination tree.
  echo "ℹ No new commits for ${BRANCH} since --last-rev (NO_OP) — creating baseline commit..."
  if [[ "${DRY_RUN}" == true ]]; then
    echo "[dry-run] Would create baseline commit on ${MIRROR_ORG}/${MIRROR_REPO}@${BRANCH}."
  else
    create_baseline_commit "${BRANCH}"
  fi
  rc=0
fi

handle_copybara_rc "${rc}" "${BRANCH}"
