#!/bin/bash
set -eoux pipefail

git config --global --add safe.directory /snowflake-cli

if [[ -z "${RELEASE_TYPE+x}" ]]; then
  echo "RELEASE_TYPE is not set"
  exit 1
fi

ROOT_DIR="$(git rev-parse --show-toplevel)"
VERSION=$(hatch version)

check_tag_version_for_release() {
  if [[ ${RELEASE_TYPE} == "release" ]]; then
    echo "checking for proper tag set to commit"

    TAG_VERSION=$(git tag --points-at HEAD | sed 's/^v//')

    if [[ -n "$TAG_VERSION" && "$TAG_VERSION" != "$VERSION" ]]; then
      echo "Tag version $TAG_VERSION does not match package version $VERSION"
      exit 1
    fi
  fi
}

check_tag_version_for_release

CLI_WHL_FILE="${ROOT_DIR}/dist/snowflake_cli-${VERSION}-py3-none-any.whl"
CLI_LABS_WHL_FILE="${ROOT_DIR}/.compat/snowflake-cli-labs/dist/snowflake_cli_labs-${VERSION}-py3-none-any.whl"

echo $CLI_WHL_FILE
echo $CLI_LABS_WHL_FILE

if [[ ! -f $CLI_WHL_FILE ]]; then
  echo "File not found: $CLI_WHL_FILE"
  exit 1
fi

if [[ ! -f $CLI_LABS_WHL_FILE ]]; then
  echo "File not found: $CLI_LABS_WHL_FILE"
  exit 1
fi

test_version() {
  cli_version=$($1 --version)
  if [[ $cli_version != "Snowflake CLI version: $VERSION" ]]; then
    echo "Version mismatch ${VERSION} != ${cli_version}"
    exit 1
  fi
  $1 --help
  # we skip connection test for now
  # due to the fact that required fuse secrets
  # do not work with job (only pipeline is supported)
  #
  # $1 connection test
  # $1 sql -q "select current_timestamp()"
}

validate_from_whl() {
  python -m venv cli_whl_venv
  source cli_whl_venv/bin/activate

  pip install $1
  test_version "./cli_whl_venv/bin/snow"

  deactivate
}

validate_from_tag() {
  python -m venv cli_tag_venv
  source cli_tag_venv/bin/activate

  pip install git+https://github.com/snowflakedb/snowflake-cli.git@v${VERSION}

  test_version ./cli_tag_venv/bin/snow
  deactivate
}

rm -rf cli_whl_venv || true
rm -rf cli_tag_venv || true

validate_from_whl $CLI_WHL_FILE
validate_from_whl $CLI_LABS_WHL_FILE

rm -rf cli_whl_venv || true
rm -rf cli_tag_venv || true
