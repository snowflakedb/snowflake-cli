#!/usr/bin/env bash

# Build Debug RPM Package for Snowflake CLI
# This script creates an RPM package with debugging symbols for Fedora

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

# Check if running on Fedora/RHEL
if ! command -v rpmbuild &> /dev/null; then
    print_error "rpmbuild not found. Please install rpm-build:"
    print_error "  sudo dnf install rpm-build rpmdevtools"
    exit 1
fi

# Get project information
PROJECT_ROOT="$(git rev-parse --show-toplevel)"
cd "$PROJECT_ROOT"

# Get version from hatch
VERSION=$(hatch version)
PACKAGE_NAME="snowflake-cli-debug"
PACKAGER_EMAIL="${PACKAGER_EMAIL:-build@snowflake.com}"
BUILD_DATE=$(date +'%a %b %d %Y')

print_header "Building Debug RPM for Snowflake CLI"

print_status "Project: $PACKAGE_NAME"
print_status "Version: $VERSION"
print_status "Build date: $BUILD_DATE"

# Setup RPM build environment
print_status "Setting up RPM build environment..."
rpmdev-setuptree

# Define directories
RPM_BUILD_DIR="$HOME/rpmbuild"
SOURCES_DIR="$RPM_BUILD_DIR/SOURCES"
SPECS_DIR="$RPM_BUILD_DIR/SPECS"
RPMS_DIR="$RPM_BUILD_DIR/RPMS"
SRPMS_DIR="$RPM_BUILD_DIR/SRPMS"

# Create source tarball
print_status "Creating source tarball..."
TARBALL_NAME="${PACKAGE_NAME}-${VERSION}.tar.gz"
TEMP_DIR=$(mktemp -d)
PACKAGE_DIR="$TEMP_DIR/${PACKAGE_NAME}-${VERSION}"

# Copy source files
cp -r "$PROJECT_ROOT" "$PACKAGE_DIR"

# Clean up unnecessary files
cd "$PACKAGE_DIR"
rm -rf .git .hatch dist __pycache__ .pytest_cache
find . -name "*.pyc" -delete
find . -name "__pycache__" -type d -exec rm -rf {} + || true

# Create tarball
cd "$TEMP_DIR"
tar -czf "$SOURCES_DIR/$TARBALL_NAME" "${PACKAGE_NAME}-${VERSION}"
rm -rf "$TEMP_DIR"

print_status "Source tarball created: $SOURCES_DIR/$TARBALL_NAME"

# Prepare spec file
print_status "Preparing RPM spec file..."
SPEC_FILE="$SPECS_DIR/${PACKAGE_NAME}.spec"

# Copy and customize spec file
sed -e "s/%{version}/$VERSION/g" \
    -e "s/%{packager_email}/$PACKAGER_EMAIL/g" \
    -e "s/%{date}/$BUILD_DATE/g" \
    "$PROJECT_ROOT/scripts/packaging/snowflake-cli-debug.spec" > "$SPEC_FILE"

print_status "Spec file prepared: $SPEC_FILE"

# Install build dependencies
print_status "Installing build dependencies..."
sudo dnf builddep -y "$SPEC_FILE" || {
    print_warning "Could not install all build dependencies automatically."
    print_warning "You may need to install them manually:"
    print_warning "  sudo dnf install python3-devel gcc gcc-c++ rust cargo gdb"
}

# Check system resources
print_status "Checking system resources..."
AVAILABLE_MEM=$(free -m | awk 'NR==2{printf "%.0f", $7}')
AVAILABLE_SWAP=$(free -m | awk 'NR==3{printf "%.0f", $2}')

print_status "Available memory: ${AVAILABLE_MEM}MB"
print_status "Available swap: ${AVAILABLE_SWAP}MB"

if [[ $AVAILABLE_MEM -lt 2048 && $AVAILABLE_SWAP -lt 1024 ]]; then
    print_warning "Low memory detected. Consider adding swap space:"
    print_warning "  sudo fallocate -l 2G /swapfile"
    print_warning "  sudo chmod 600 /swapfile"
    print_warning "  sudo mkswap /swapfile && sudo swapon /swapfile"
fi

# Build RPM
print_header "Building Debug RPM Package"

print_status "Starting RPM build process..."
print_warning "This may take 10-30 minutes depending on your system..."

# Build with low-memory settings
export CARGO_BUILD_JOBS=1
export RUSTFLAGS="-C opt-level=1 -C debuginfo=2 -C lto=off"

if rpmbuild -ba "$SPEC_FILE"; then
    print_header "Build Successful!"

    # Find built packages
    RPM_ARCH=$(uname -m)
    MAIN_RPM=$(find "$RPMS_DIR" -name "${PACKAGE_NAME}-${VERSION}-*.${RPM_ARCH}.rpm" | head -1)
    DEBUGINFO_RPM=$(find "$RPMS_DIR" -name "${PACKAGE_NAME}-debuginfo-${VERSION}-*.${RPM_ARCH}.rpm" | head -1)
    SOURCE_RPM=$(find "$SRPMS_DIR" -name "${PACKAGE_NAME}-${VERSION}-*.src.rpm" | head -1)

    print_status "Built packages:"
    if [[ -f "$MAIN_RPM" ]]; then
        print_status "  Main package: $MAIN_RPM"
        ls -lh "$MAIN_RPM"
    fi

    if [[ -f "$DEBUGINFO_RPM" ]]; then
        print_status "  Debug info: $DEBUGINFO_RPM"
        ls -lh "$DEBUGINFO_RPM"
    fi

    if [[ -f "$SOURCE_RPM" ]]; then
        print_status "  Source RPM: $SOURCE_RPM"
        ls -lh "$SOURCE_RPM"
    fi

    print_header "Installation Instructions"
    print_status "To install the debug package:"
    print_status "  sudo dnf install $MAIN_RPM"
    print_status ""
    print_status "To install with debug symbols:"
    print_status "  sudo dnf install $MAIN_RPM $DEBUGINFO_RPM"
    print_status ""
    print_status "To debug the installed package:"
    print_status "  gdb /usr/bin/snow-debug"
    print_status "  (gdb) run --help"
    print_status ""
    print_status "Documentation will be available at:"
    print_status "  /usr/share/doc/snowflake-cli-debug/"

else
    print_error "RPM build failed!"
    print_error "Check the build logs above for details."
    print_error ""
    print_error "Common issues and solutions:"
    print_error "1. Out of memory: Add swap space or reduce CARGO_BUILD_JOBS"
    print_error "2. Missing dependencies: Install with 'sudo dnf builddep $SPEC_FILE'"
    print_error "3. Rust compilation issues: Try 'rustup update'"
    exit 1
fi

print_header "Build Complete"
print_status "🎉 Debug RPM package built successfully!"
print_status "Packages are located in: $RPMS_DIR"
