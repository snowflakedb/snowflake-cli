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

RUN gem install dotenv:2.8.1 fpm

ENV PYTHON_VERSION=3.10.16
ENV PYTHON_PREFIX=/usr/local

# Set conservative compilation flags optimized for size and compatibility
# Avoid modern CPU instructions like AVX, AVX2, AVX512
ENV CFLAGS="-Os -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f -ffunction-sections -fdata-sections"
ENV CXXFLAGS="-Os -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f -ffunction-sections -fdata-sections"
ENV LDFLAGS="-Wl,--gc-sections -s"

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
        --with-ensurepip=install \
        --without-doc-strings \
        --disable-test-modules \
        --without-debug-build \
        --disable-ipv6 \
        --without-pymalloc

# Build and install Python
RUN cd /tmp/Python-${PYTHON_VERSION} && \
    echo "Building Python with conservative CPU flags..." && \
    make -j$(nproc) && \
    echo "Installing Python..." && \
    make altinstall

# Clean up
RUN rm -rf /tmp/Python-${PYTHON_VERSION}*

# Aggressive size optimization
# Remove unnecessary files and directories
RUN rm -rf ${PYTHON_PREFIX}/lib/python3.10/test
RUN rm -rf ${PYTHON_PREFIX}/lib/python3.10/*/test*
RUN rm -rf ${PYTHON_PREFIX}/lib/python3.10/__pycache__
RUN find ${PYTHON_PREFIX}/lib/python3.10 -name "*.pyc" -delete
RUN find ${PYTHON_PREFIX}/lib/python3.10 -name "*.pyo" -delete
RUN find ${PYTHON_PREFIX}/lib/python3.10 -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Remove unused standard library modules that are confirmed not used by snowflake-cli
RUN cd ${PYTHON_PREFIX}/lib/python3.10 && \
    rm -rf \
    tkinter \
    turtle* \
    idlelib \
    lib2to3 \
    pydoc_data \
    ensurepip/_bundled

# Additional cleanup for smaller size
RUN find ${PYTHON_PREFIX} -name "*.a" -delete  # Remove static libraries
RUN find ${PYTHON_PREFIX} -name "*.la" -delete  # Remove libtool files
RUN rm -rf ${PYTHON_PREFIX}/share/man  # Remove manual pages
RUN rm -rf ${PYTHON_PREFIX}/include  # Remove header files
RUN rm -rf ${PYTHON_PREFIX}/lib/pkgconfig  # Remove pkg-config files

# Remove more standard library components that may not be needed
RUN cd ${PYTHON_PREFIX}/lib/python3.10 && \
    rm -rf \
    ensurepip \
    venv \
    site-packages/pip* \
    site-packages/setuptools* \
    site-packages/_distutils_hack

# Strip debug symbols to reduce size
RUN find ${PYTHON_PREFIX} -name "*.so" -exec strip {} \; 2>/dev/null || true
RUN strip ${PYTHON_PREFIX}/bin/python3.10 2>/dev/null || true

# Create symlinks for python and pip
RUN ln -sf ${PYTHON_PREFIX}/bin/python3.10 ${PYTHON_PREFIX}/bin/python
RUN ln -sf ${PYTHON_PREFIX}/bin/python3.10 ${PYTHON_PREFIX}/bin/python3
RUN ln -sf ${PYTHON_PREFIX}/bin/pip3.10 ${PYTHON_PREFIX}/bin/pip
RUN ln -sf ${PYTHON_PREFIX}/bin/pip3.10 ${PYTHON_PREFIX}/bin/pip3

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
