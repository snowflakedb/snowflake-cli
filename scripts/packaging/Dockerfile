FROM artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/docker-remote/library/ubuntu:18.04 AS base

ENV HOME=/root

RUN apt -y update
RUN apt -y install -y software-properties-common binutils --fix-missing
RUN add-apt-repository ppa:deadsnakes/ppa

# Essential build tools and Python compilation dependencies
RUN apt install -y --fix-missing \
  build-essential \
  curl \
  git \
  pkg-config \
  libssl-dev \
  zlib1g-dev \
  libbz2-dev \
  libreadline-dev \
  libffi-dev \
  liblzma-dev \
  libncursesw5-dev \
  libssl1.1 \
  zlib1g \
  libbz2-1.0 \
  liblzma5 \
  libffi6 \
  ruby \
  squashfs-tools \
  rpm

RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt install -y tk-dev

RUN gem install dotenv -v 2.8.1 fpm

ENV PYTHON_VERSION=3.10.16
ENV PYTHON_PREFIX=/usr/local

# Set conservative compilation flags for maximum CPU compatibility
# Avoid modern CPU instructions like AVX, AVX2, AVX512
ENV CFLAGS="-O2 -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f"
ENV CXXFLAGS="-O2 -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f"

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
        --with-ensurepip=install

# Build and install Python
RUN cd /tmp/Python-${PYTHON_VERSION} && \
    echo "Building Python with conservative CPU flags..." && \
    make -j$(nproc) && \
    echo "Installing Python..." && \
    make install

# Clean up
RUN rm -rf /tmp/Python-${PYTHON_VERSION}*

# Update PATH to use our custom Python
ENV PATH="${PYTHON_PREFIX}/bin:$PATH"

# Update library path for shared libraries
RUN echo "${PYTHON_PREFIX}/lib" > /etc/ld.so.conf.d/python.conf && ldconfig

# Install packages with selective binary policy
RUN python -m pip install \
    --only-binary=cryptography,cffi,pycparser,setuptools-rust,pyOpenSSL \
    --no-binary=uv,hatch \
    -U uv hatch

WORKDIR /snowflake-cli
