#!/usr/bin/env bash
# Sourced by callers, or executed directly with `bash copybara-verify.sh`.
#
# Runs the Copybara verify workflow against the git repo at REPO_ROOT.
# Copybara's git.origin checks out the HEAD tree (same as `mirror`);
# no destination credentials.
#
# Requires (when sourced):
#   SCRIPT_DIR        absolute path to scripts/mirroring/
#
# Optional:
#   COPYBARA_IMAGE    docker image name (default: copybara:local)
#   REPO_ROOT         git repo root (default: toplevel of SCRIPT_DIR's repo)

_LIB_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
# shellcheck source=config.sh
source "${_LIB_DIR}/config.sh"

# copybara_verify
#
# Returns 0 for success/NO_OP, or the Copybara exit code on failure.
copybara_verify() {
  local script_dir="${SCRIPT_DIR:-$(cd -- "${_LIB_DIR}/.." && pwd)}"
  local image="${COPYBARA_IMAGE:-copybara:local}"

  command -v git >/dev/null 2>&1 || { echo "ERROR: git is not installed" >&2; return 1; }
  command -v docker >/dev/null 2>&1 || { echo "ERROR: docker is not installed" >&2; return 1; }

  local repo_root="${REPO_ROOT:-$(git -C "${script_dir}" rev-parse --show-toplevel)}"

  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    echo "ERROR: docker image '${image}' not found. Build it first." >&2
    return 1
  fi

  local rc=0
  docker run --rm \
    -v "${script_dir}/copybara:/workdir" \
    -v "${repo_root}:/origin:ro" \
    "${image}" \
    migrate copy.bara.sky "${COPYBARA_VERIFY_WORKFLOW}" HEAD \
    --folder-dir /tmp/copybara-verify-out \
    --force \
    || rc=$?

  case "${rc}" in
    0) echo "✓ Copybara verify passed" ;;
    4) echo "ℹ Copybara verify produced no changes (NO_OP)" ;;
    *) echo "✗ Copybara verify failed (exit ${rc})" >&2; return "${rc}" ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  set -euo pipefail
  SCRIPT_DIR="${SCRIPT_DIR:-$(cd -- "${_LIB_DIR}/.." && pwd)}"
  copybara_verify "$@"
fi
