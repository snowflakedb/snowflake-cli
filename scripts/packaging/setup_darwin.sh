#!/usr/bin/env bash
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

function ensure_pyenv_installation() {
  if ! command -v pyenv &>/dev/null; then
    echo "pyenv not found, installing..."
    arch -$(uname -m) brew install pyenv
  else
    echo "pyenv already installed"
  fi

  export PYENV_ROOT="$HOME/.pyenv"
  [[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init - bash)"
}

function ensure_python_installation() {
  pyenv versions
  pyenv install -s 3.10
  pyenv install -s 3.11
  pyenv global 3.11
  python --version
}
# python -m venv venv
# . venv/bin/activate
# pip install -U pip hatch uv pyinstaller
# hatch run packaging:package-darwin-binaries
