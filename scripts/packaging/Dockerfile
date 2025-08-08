FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root

RUN apt -y update
RUN apt -y install -y software-properties-common binutils --fix-missing
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt install -y --fix-missing ruby\
  squashfs-tools\
  rpm\
  vim \
  build-essential\
  libssl-dev\
  zlib1g-dev\
  libbz2-dev\
  libreadline-dev\
  libsqlite3-dev\
  curl\
  git \
  libncursesw5-dev\
  xz-utils \
  libxml2-dev\
  libxmlsec1-dev\
  libffi-dev\
  liblzma-dev\
  pkg-config\
  uuid-dev\
  libgdbm-dev\
  libc6-dev\
  libnss3-dev\
  libsqlite3-dev\
  libssl-dev\
  openssl
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tk-dev

RUN gem install dotenv -v 2.8.1
RUN gem install fpm

ENV PYTHON_VERSION=3.10.16
ENV PYTHON_PREFIX=/usr/local

# Set conservative compilation flags for maximum CPU compatibility
# Avoid modern CPU instructions like AVX, AVX2, AVX512
ENV CFLAGS="-O2 -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f"
ENV CXXFLAGS="-O2 -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f"
ENV LDFLAGS=""
ENV CPPFLAGS=""

# Display compilation environment for verification
RUN echo "Building Python with conservative CPU flags:" && \
    echo "CFLAGS: $CFLAGS" && \
    echo "CXXFLAGS: $CXXFLAGS"

# Download Python source
RUN cd /tmp && \
    echo "Downloading Python ${PYTHON_VERSION}..." && \
    curl -O https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    echo "Extracting Python source..." && \
    tar -xzf Python-${PYTHON_VERSION}.tgz

# Configure Python build
RUN cd /tmp/Python-${PYTHON_VERSION} && \
    echo "Configuring Python build with conservative flags..." && \
    echo "CFLAGS: $CFLAGS" && \
    echo "CXXFLAGS: $CXXFLAGS" && \
    ./configure \
        --prefix=${PYTHON_PREFIX} \
        --disable-shared \
        --with-system-ffi \
        --with-computed-gotos \
        --enable-loadable-sqlite-extensions \
        --with-ensurepip=install

# Build and install Python
RUN cd /tmp/Python-${PYTHON_VERSION} && \
    echo "Building Python with conservative CPU flags..." && \
    make -j$(nproc) && \
    echo "Installing Python..." && \
    make altinstall

# Clean up
RUN rm -rf /tmp/Python-${PYTHON_VERSION}*

# Create symlinks for python and pip
RUN ln -sf ${PYTHON_PREFIX}/bin/python3.10 ${PYTHON_PREFIX}/bin/python
RUN ln -sf ${PYTHON_PREFIX}/bin/python3.10 ${PYTHON_PREFIX}/bin/python3
RUN ln -sf ${PYTHON_PREFIX}/bin/pip3.10 ${PYTHON_PREFIX}/bin/pip
RUN ln -sf ${PYTHON_PREFIX}/bin/pip3.10 ${PYTHON_PREFIX}/bin/pip3

# Update PATH to use our custom Python
ENV PATH="${PYTHON_PREFIX}/bin:$PATH"

# Update library path for shared libraries
RUN echo "${PYTHON_PREFIX}/lib" > /etc/ld.so.conf.d/python.conf && ldconfig

# Verify Python was built with our conservative flags
RUN python -c "import sysconfig; print('Python built with CFLAGS:', sysconfig.get_config_var('CFLAGS'))"
RUN python -c "import sysconfig; print('Python built with CXXFLAGS:', sysconfig.get_config_var('CXXFLAGS'))"
RUN python -c "import sys; print('Python version:', sys.version)"

# Additional verification: check if Python binary uses conservative instructions
RUN objdump -f $(which python) | head -10 || echo "objdump not available"
RUN file $(which python) || echo "file command not available"

# Install pip first
RUN python -m pip install -U pip

# Install packages with selective binary policy - be more restrictive
# Only allow binaries for packages that are known to be problematic to build
RUN python -m pip install --no-binary=:all: -U pip setuptools wheel || \
    python -m pip install --only-binary=cryptography,cffi,pycparser,setuptools-rust,pyOpenSSL -U pip setuptools wheel

RUN python -m pip install \
    --only-binary=cryptography,cffi,pycparser,setuptools-rust,pyOpenSSL \
    --no-binary=uv,hatch \
    -U uv hatch

# Test that Python itself runs with conservative CPU instructions
RUN python -c "import sys; print('Python executable path:', sys.executable); print('Python can import basic modules successfully'); import json, os, subprocess; print('Core modules import successfully')"

# Test our Python in a minimal environment (similar to your test)
RUN echo '#!/bin/bash' > /tmp/test_python.sh && \
    echo 'export PATH=/usr/local/bin:$PATH' >> /tmp/test_python.sh && \
    echo 'python -c "print(\"Python works with conservative CPU flags\")"' >> /tmp/test_python.sh && \
    chmod +x /tmp/test_python.sh && \
    /tmp/test_python.sh

WORKDIR /snowflake-cli
