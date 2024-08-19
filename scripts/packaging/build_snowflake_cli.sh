#!/bin/bash

set -o pipefail

# Set up the environment
# check for pyenv
# - pyenv
# - uv
# - hatch
# - pyinstaller

ROOT_DIR=$(git rev-parse HEAD)
