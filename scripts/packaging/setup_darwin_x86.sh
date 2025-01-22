#!/bin/bash -e
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

arch -$(uname -m) brew install pyenv

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"

pyenv versions
pyenv install -s 3.10
pyenv shell 3.10

python -m venv venv
. venv/bin/activate
pip install -U pip hatch uv pyinstaller
hatch run packaging:package-darwin-binaries
