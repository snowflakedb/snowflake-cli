#!/usr/bin/env bash
# Scheduled incremental mirror of a named branch (release-v*, custom branches).
# Automatically seeds the mirror branch on first run:
#   1. Checks via gh api whether the branch already exists on the mirror.
#   2. Finds the source fork point (merge-base with main) via gh api.
#   3. Paginates mirror's main commits via gh api to find the one whose
#      GitOrigin-RevId trailer matches the fork point SHA.
#   4. Creates the mirror branch at that commit via gh api.
#   5. Runs Copybara — which finds GitOrigin-RevId on the branch tip and proceeds.
#
# All GitHub operations use gh api (not git-over-HTTPS) to avoid libcurl version
# mismatches with Nix-managed git on the host.
#
# If the fork point is not yet on the mirror's main, the script fails fast with
# instructions to run mirror-main.sh first. No --force or --init-history is used.
#
# Usage: mirror-branch.sh --branch <name> [--dry-run]
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
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)  BRANCH="${2:?ERROR: --branch requires a value}"; shift 2 ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) echo "ERROR: unknown flag: $1" >&2; exit 1 ;;
  esac
done

# ---------- validation ----------
[[ -z "${SOURCE_GH_TOKEN:-}" ]] && { echo "ERROR: SOURCE_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${MIRROR_GH_TOKEN:-}" ]] && { echo "ERROR: MIRROR_GH_TOKEN is not set" >&2; exit 1; }
[[ -z "${BRANCH}"            ]] && { echo "ERROR: --branch is required" >&2;        exit 1; }
[[ "${BRANCH}" == "main"     ]] && { echo "ERROR: use mirror-main.sh for the main branch" >&2; exit 1; }

# ---------- seeding ----------
if ! GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
     "repos/${MIRROR_ORG}/${MIRROR_REPO}/branches/${BRANCH}" \
     >/dev/null 2>&1; then
  echo "Branch '${BRANCH}' not found on mirror — attempting to seed it."

  # Fork point: last common ancestor of source main and the release branch.
  forkpoint=$(GH_TOKEN="${SOURCE_GH_TOKEN}" gh api \
    "repos/${SOURCE_ORG}/${SOURCE_REPO}/compare/main...${BRANCH}" \
    --jq '.merge_base_commit.sha' 2>/dev/null) \
    || { echo "ERROR: cannot find '${BRANCH}' on source — does it exist?" >&2; exit 1; }
  [[ -z "${forkpoint}" ]] \
    && { echo "ERROR: cannot determine fork point for '${BRANCH}'" >&2; exit 1; }

  # Find the destination commit that corresponds to the fork point via the
  # GitOrigin-RevId trailer Copybara stamps on every mirrored main commit.
  echo "Searching mirror's main for GitOrigin-RevId: ${forkpoint}..."
  seed=$(GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/commits?sha=main&per_page=100" \
    --paginate \
    --jq ".[] | select(.commit.message | contains(\"GitOrigin-RevId: ${forkpoint}\")) | .sha" \
    | head -1 || true)

  if [[ -z "${seed}" ]]; then
    echo "ERROR: fork point ${forkpoint} is not yet on the mirror's main." >&2
    echo "       Run mirror-main.sh first, then retry." >&2
    exit 1
  fi

  if [[ "${DRY_RUN}" == true ]]; then
    echo "[dry-run] '${BRANCH}' is not on the mirror."
    echo "[dry-run] Would seed it from destination commit ${seed}"
    echo "[dry-run]   (corresponds to source fork point ${forkpoint})"
    echo "[dry-run] Skipping Copybara replay preview — seed must exist on mirror first."
    exit 0
  fi

  echo "Seeding '${BRANCH}' on mirror from commit ${seed} (fork point ${forkpoint})."
  GH_TOKEN="${MIRROR_GH_TOKEN}" gh api \
    "repos/${MIRROR_ORG}/${MIRROR_REPO}/git/refs" \
    -f "ref=refs/heads/${BRANCH}" \
    -f "sha=${seed}" \
    >/dev/null
  echo "Seed pushed — Copybara will resume from the GitOrigin-RevId on the branch tip."
fi

# ---------- run Copybara ----------
# The destination branch now has a GitOrigin-RevId on its tip (either from the
# seed step above or from a previous mirror run). Copybara auto-detects the
# baseline from that trailer — no --force or --init-history needed.
extra_args=("--git-destination-push=refs/heads/${BRANCH}")
[[ "${DRY_RUN}" == true ]] && extra_args+=("--dry-run")

rc=0
copybara_run "${COPYBARA_WORKFLOW}" "refs/heads/${BRANCH}" "${extra_args[@]}" || rc=$?
handle_copybara_rc "${rc}" "${BRANCH}"
