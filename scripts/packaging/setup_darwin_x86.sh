#!/usr/bin/env bash
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

arch -$(uname -m) brew install pyenv

export PYENV_ROOT="$HOME/.pyenv"
[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init - bash)"

pyenv versions
arch -$(uname -m) pyenv install -s 3.10
arch -$(uname -m) pyenv install -s 3.11
pyenv global 3.11
python --version
# python -m venv venv
# . venv/bin/activate
# pip install -U pip hatch uv pyinstaller
# hatch run packaging:package-darwin-binaries
