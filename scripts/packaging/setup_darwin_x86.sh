#!/bin/bash -e
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

brew install pyenv

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"

pyenv versions
pyenv install -s 3.11
pyenv shell 3.11

python -m venv venv
. venv/bin/activate
pip install -U pip hatch uv pyinstaller
hatch run packaging:build-binaries
