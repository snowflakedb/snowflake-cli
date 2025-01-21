#!/bin/bash -e
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

pyenv versions
pyenv install 3.10
pyenv shell 3.10

python -m venv venv
. venv/bin/activate
pip install -U pip hatch uv pyinstaller
hatch run packaging:build-binaries
