#!/usr/bin/env bash
set -o pipefail

echo "Setting up the Snowflake CLI build environment"

ensure_pyenv_installation() {
  if ! command -v pyenv &>/dev/null; then
    echo "pyenv not found, installing..."
    arch -$(uname -m) brew install pyenv
  else
    echo "pyenv already installed"
  fi
}

activate_pyenv() {
  export PYENV_ROOT="$HOME/.pyenv"
  [[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"
  eval "$(pyenv init - bash)"
}

ensure_hatch_installation() {
  if ! command -v hatch &>/dev/null; then
    echo "hatch not found, installing..."
    arch -$(uname -m) install hatch
  else
    echo "hatch already installed"
    arch -$(uname -m) brew upgrade hatch
  fi
}

ensure_python_installation() {
  pyenv versions
  pyenv install -s 3.10
  pyenv install -s 3.11
  pyenv global 3.11
  python --version
}

# ensure_pyenv_installation
# activate_pyenv
# ensure_python_installation
# ensure_hatch_installation
