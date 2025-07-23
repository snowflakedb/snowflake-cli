FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/redhat/ubi9:latest AS base

ENV HOME=/root

# Enable additional repositories for UBI9
RUN dnf install -y dnf-plugins-core
RUN dnf config-manager --set-enabled codeready-builder-for-rhel-9-x86_64-rpms || true

# Install EPEL for additional packages
RUN dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm || true

# Update system
RUN dnf update -y

# Install core development tools (use --allowerasing to resolve curl conflicts in UBI9)
RUN dnf install -y --allowerasing \
  gcc \
  gcc-c++ \
  make \
  autoconf \
  automake \
  libtool \
  pkgconf-pkg-config \
  ruby \
  ruby-devel \
  rpm-build \
  vim \
  curl \
  git \
  patch \
  which

# Install development libraries
RUN dnf install -y \
  openssl-devel \
  zlib-devel \
  bzip2-devel \
  sqlite-devel \
  ncurses-devel \
  xz \
  libxml2-devel \
  libffi-devel \
  xz-devel \
  gdb \
  strace \
  binutils

# Install packages that may need EPEL
RUN dnf install -y squashfs-tools || echo "squashfs-tools not available, continuing..."

# Try to install readline-devel (might be in different package)
RUN dnf install -y readline-devel || dnf install -y libedit-devel || echo "readline dev package not found"

# Try to install tkinter development (might not be needed for headless)
RUN dnf install -y tkinter || dnf install -y python3-tkinter || echo "tkinter not available, continuing..."

# Install Ruby gems (UBI9 has modern Ruby that supports current gem versions)
RUN gem install fpm

# Install additional dependencies for Python compilation
RUN dnf install -y \
  python3-devel \
  python3-pip \
  libuuid-devel

# Try to install gdbm-devel (package name might be different in UBI9)
RUN dnf install -y gdbm-devel || dnf install -y libgdbm-devel || echo "gdbm development package not found, continuing..."

# Try to install libnsl2-devel (might have different name or not be needed)
RUN dnf install -y libnsl2-devel || dnf install -y libnsl-devel || echo "libnsl development package not found, continuing..."

# Setup pyenv for Python version management
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:$PATH"
ENV PYTHON_VERSION=3.10

# Install pyenv and Python
RUN curl https://pyenv.run | bash
RUN pyenv install ${PYTHON_VERSION}
RUN pyenv rehash
RUN pyenv global ${PYTHON_VERSION}
RUN pyenv versions
RUN pyenv rehash
RUN pyenv exec pip install -U pip uv hatch

# Install Rust and Cargo
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustc --version && cargo --version

WORKDIR /snowflake-cli
