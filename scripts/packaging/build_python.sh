#!/bin/bash
set -e

# Build Python with conservative CPU flags for maximum compatibility
# This script is designed to be run in a Docker container

PYTHON_VERSION=3.12.16
PYTHON_PREFIX=/usr/local

echo "=== Starting Python build process ==="
echo "Python version: $PYTHON_VERSION"
echo "Install prefix: $PYTHON_PREFIX"
echo "Architecture: $(uname -m)"

# Download Python source
echo "=== Downloading Python source ==="
cd /tmp
echo "Downloading Python ${PYTHON_VERSION}..."
curl -O https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz
echo "Extracting Python source..."
tar -xzf Python-${PYTHON_VERSION}.tgz

# Configure Python build with architecture-specific conservative flags
echo "=== Configuring Python build ==="
cd /tmp/Python-${PYTHON_VERSION}
ARCH=$(uname -m)
echo "Configuring Python build for architecture: $ARCH"

if [ "$ARCH" = "x86_64" ]; then
    export CFLAGS="-Os -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f -ffunction-sections -fdata-sections"
    export CXXFLAGS="-Os -march=x86-64 -mtune=generic -mno-avx -mno-avx2 -mno-avx512f -ffunction-sections -fdata-sections"
elif [ "$ARCH" = "aarch64" ]; then
    export CFLAGS="-Os -march=armv8-a -mtune=generic -ffunction-sections -fdata-sections"
    export CXXFLAGS="-Os -march=armv8-a -mtune=generic -ffunction-sections -fdata-sections"
else
    export CFLAGS="-Os -mtune=generic -ffunction-sections -fdata-sections"
    export CXXFLAGS="-Os -mtune=generic -ffunction-sections -fdata-sections"
fi

export LDFLAGS="-Wl,--gc-sections -s"

echo "CFLAGS: $CFLAGS"
echo "CXXFLAGS: $CXXFLAGS"
echo "LDFLAGS: $LDFLAGS"

# 3.12 detects libffi with pkg-config, so --with-system-ffi is gone.
# The default build is not a debug build; --without-debug-build is not a configure option.
./configure \
    --prefix=${PYTHON_PREFIX} \
    --disable-shared \
    --with-computed-gotos \
    --with-ensurepip=install \
    --without-doc-strings \
    --disable-test-modules \
    --disable-ipv6 \
    --without-pymalloc

# Build and install Python
echo "=== Building Python ==="
echo "Building Python with conservative CPU flags..."
make -j$(nproc)
echo "Installing Python..."
make altinstall

# Clean up source
echo "=== Cleaning up source files ==="
rm -rf /tmp/Python-${PYTHON_VERSION}*

# Aggressive size optimization
echo "=== Starting size optimization ==="

# Remove unnecessary files and directories
echo "Removing test modules and cache files..."
rm -rf ${PYTHON_PREFIX}/lib/python3.12/test
rm -rf ${PYTHON_PREFIX}/lib/python3.12/*/test*
rm -rf ${PYTHON_PREFIX}/lib/python3.12/__pycache__
find ${PYTHON_PREFIX}/lib/python3.12 -name "*.pyc" -delete
find ${PYTHON_PREFIX}/lib/python3.12 -name "*.pyo" -delete
find ${PYTHON_PREFIX}/lib/python3.12 -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Remove unused standard library modules that are confirmed not used by snowflake-cli
echo "Removing unused standard library modules..."
cd ${PYTHON_PREFIX}/lib/python3.12
rm -rf \
    tkinter \
    turtle* \
    idlelib \
    lib2to3 \
    pydoc_data

# Additional cleanup for smaller size
echo "Removing development files..."
find ${PYTHON_PREFIX} -name "*.a" -delete  # Remove static libraries
find ${PYTHON_PREFIX} -name "*.la" -delete  # Remove libtool files
rm -rf ${PYTHON_PREFIX}/share/man  # Remove manual pages
rm -rf ${PYTHON_PREFIX}/include  # Remove header files
rm -rf ${PYTHON_PREFIX}/lib/pkgconfig  # Remove pkg-config files


# Strip debug symbols to reduce size
echo "Stripping debug symbols..."
find ${PYTHON_PREFIX} -name "*.so" -exec strip {} \; 2>/dev/null || true
strip ${PYTHON_PREFIX}/bin/python3.12 2>/dev/null || true

# Create symlinks for python and pip
echo "Creating symlinks..."
ln -sf ${PYTHON_PREFIX}/bin/python3.12 ${PYTHON_PREFIX}/bin/python
ln -sf ${PYTHON_PREFIX}/bin/python3.12 ${PYTHON_PREFIX}/bin/python3
ln -sf ${PYTHON_PREFIX}/bin/pip3.12 ${PYTHON_PREFIX}/bin/pip
ln -sf ${PYTHON_PREFIX}/bin/pip3.12 ${PYTHON_PREFIX}/bin/pip3

# Update library path for shared libraries
echo "Updating library configuration..."
echo "${PYTHON_PREFIX}/lib" > /etc/ld.so.conf.d/python.conf
ldconfig

# Display final size information
echo "=== Build completed successfully ==="
echo "Python installation size:"
du -sh ${PYTHON_PREFIX}
echo "Python binary size:"
ls -lh ${PYTHON_PREFIX}/bin/python3.12
echo "Python version check:"
${PYTHON_PREFIX}/bin/python3.12 --version

echo "=== Python build process completed ==="
