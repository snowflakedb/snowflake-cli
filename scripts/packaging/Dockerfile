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
ENV CFLAGS="-O2 -march=x86-64 -mtune=generic"
ENV CXXFLAGS="-O2 -march=x86-64 -mtune=generic"
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
        --enable-shared \
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
RUN python -c "import sys; print('Python version:', sys.version)"

# Install pip first
RUN python -m pip install -U pip

# Install packages with selective binary policy
# Allow binaries for complex packages like cryptography but build others from source
RUN python -m pip install \
    --only-binary=cryptography,cffi,pycparser,setuptools-rust \
    --no-binary=uv,hatch \
    -U uv hatch

WORKDIR /snowflake-cli
