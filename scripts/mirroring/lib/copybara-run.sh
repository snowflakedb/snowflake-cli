#!/usr/bin/env bash
# Sourced by core mirroring scripts — do not execute directly.
#
# Requires (set by parent script):
#   SCRIPT_DIR        absolute path to scripts/mirroring/ (mounted as Copybara workdir)
#   SOURCE_GH_TOKEN   token for internal source repo
#   MIRROR_GH_TOKEN   token for public destination repo
#
# Optional:
#   COPYBARA_IMAGE    docker image name (default: copybara:local)
#   MIRROR_REPO_URL   override full mirror URL (default: derived from lib/config.sh)

_LIB_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=config.sh
source "${_LIB_DIR}/config.sh"

# copybara_run <workflow> [copybara_args...]
#
# Runs Copybara in Docker. All args after <workflow> are appended to the
# Copybara migrate command after the workflow name, so positional source_ref
# must come before flag args. Always pass fully qualified refs to avoid
# ambiguity with same-named tags
# (e.g. copybara_run mirror refs/heads/release-1.0 --git-destination-push=refs/heads/release-1.0).
# --git-destination-url is always appended last by this function.
#
# Returns the Copybara exit code (4 = NO_OP is a valid success; caller decides).
copybara_run() {
  local workflow="$1"; shift

  local image="${COPYBARA_IMAGE:-copybara:local}"
  local mirror_url="${MIRROR_REPO_URL:-https://x-access-token:${MIRROR_GH_TOKEN}@github.com/${MIRROR_ORG}/${MIRROR_REPO}.git}"

  local tmp_gitconfig
  tmp_gitconfig=$(mktemp)

  git config --file="${tmp_gitconfig}" user.name  "${MIRROR_BOT_NAME}"
  git config --file="${tmp_gitconfig}" user.email "${MIRROR_BOT_EMAIL}"

  local source_b64
  source_b64=$(printf 'x-access-token:%s' "${SOURCE_GH_TOKEN}" | base64 | tr -d '\n')
  git config --file="${tmp_gitconfig}" \
    "http.https://github.com/${SOURCE_ORG}/.extraHeader" \
    "Authorization: Basic ${source_b64}"

  local rc=0
  docker run --rm \
    -v "${SCRIPT_DIR}/copybara:/workdir" \
    -v "${tmp_gitconfig}:/root/.gitconfig:ro" \
    -e "GITHUB_TOKEN=${SOURCE_GH_TOKEN}" \
    "${image}" \
    migrate copy.bara.sky "${workflow}" "$@" \
    --git-destination-url="${mirror_url}" \
    || rc=$?

  rm -f "${tmp_gitconfig}"
  return "${rc}"
}

# handle_copybara_rc <rc> <branch>
#
# Prints a human-readable result line and returns 0 for success/NO_OP,
# or the original rc for any other exit code (triggers set -e in callers).
handle_copybara_rc() {
  local rc="$1"
  local branch="$2"
  case "${rc}" in
    0) echo "✓ Mirrored ${branch}" ;;
    4) echo "ℹ No new changes for ${branch} (NO_OP)" ;;
    *) echo "✗ Copybara failed for ${branch} (exit ${rc})" >&2; return "${rc}" ;;
  esac
}
