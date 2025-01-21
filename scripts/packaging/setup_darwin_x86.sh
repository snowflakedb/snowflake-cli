#!/bin/bash -e
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

brew install pyenv
pyenv versions
pyenv install 3.11
pyenv shell 3.11

python -m venv venv
. venv/bin/activate
pip install -U pip hatch uv pyinstaller
hatch run packaging:build-binaries
