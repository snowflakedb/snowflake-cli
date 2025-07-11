#!/usr/bin/env bash

# Fedora Debug Build Setup Script
# This script sets up a Fedora system for building and debugging the Snowflake CLI

set -euo pipefail

echo "🐧 Setting up Fedora system for Snowflake CLI debug build..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Check if running on Fedora
if ! grep -q "Fedora" /etc/os-release 2>/dev/null; then
    print_warning "This script is designed for Fedora. You may need to adapt it for your distribution."
fi

# Check if running as root
if [[ $EUID -eq 0 ]]; then
    print_error "This script should not be run as root. Please run as a regular user."
    exit 1
fi

print_header "Installing System Dependencies"

# Update system
print_status "Updating system packages..."
sudo dnf update -y

# Install build dependencies
print_status "Installing build dependencies..."
sudo dnf install -y \
    gdb \
    python3-devel \
    python3-pip \
    gcc \
    gcc-c++ \
    make \
    git \
    curl \
    tar \
    bzip2 \
    which

# Install Python debug symbols (optional but recommended)
print_status "Installing Python debug symbols..."
sudo dnf install -y python3-debuginfo python3-debug || {
    print_warning "Could not install Python debug symbols. Debugging may be limited."
}

# Install additional debug tools
print_status "Installing additional debugging tools..."
sudo dnf install -y valgrind strace ltrace || {
    print_warning "Could not install some debugging tools. Basic debugging will still work."
}

# Install Development Tools group
print_status "Installing Development Tools group..."
sudo dnf groupinstall -y "Development Tools" || {
    print_warning "Could not install Development Tools group. Manual tool installation may be needed."
}

print_header "Setting up Rust"

# Check if Rust is already installed
if command -v rustc &> /dev/null; then
    print_status "Rust is already installed: $(rustc --version)"
else
    print_status "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source ~/.cargo/env
    print_status "Rust installed: $(rustc --version)"
fi

print_header "Setting up Python Environment"

# Install hatch
print_status "Installing hatch..."
pip3 install --user hatch

# Add ~/.local/bin to PATH if not already there
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    print_status "Adding ~/.local/bin to PATH..."
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    export PATH="$HOME/.local/bin:$PATH"
fi

print_header "Configuring GDB"

# Check if GDB Python debugging extension is available
if ls /usr/share/gdb/auto-load/usr/bin/python* &> /dev/null; then
    print_status "GDB Python debugging extension is available"
else
    print_warning "GDB Python debugging extension not found. Python debugging may be limited."
fi

# Configure core dumps
print_status "Configuring core dumps..."
echo "ulimit -c unlimited" >> ~/.bashrc
sudo sysctl kernel.core_pattern=/tmp/core.%e.%p.%h.%t || {
    print_warning "Could not configure core dump pattern. You may need to do this manually."
}

print_header "Setting up Environment Variables"

# Add debug environment variables to bashrc
print_status "Adding debug environment variables to ~/.bashrc..."
cat >> ~/.bashrc << 'EOF'

# Snowflake CLI Debug Environment
export RUST_BACKTRACE=full
export PYTHONDEVMODE=1
export PYTHONDONTWRITEBYTECODE=1
export SNOWFLAKE_CLI_DEBUG=1
EOF

print_header "Checking SELinux Configuration"

# Check SELinux status
if command -v getenforce &> /dev/null; then
    SELINUX_STATUS=$(getenforce)
    print_status "SELinux status: $SELINUX_STATUS"

    if [[ "$SELINUX_STATUS" == "Enforcing" ]]; then
        print_warning "SELinux is in enforcing mode. If you encounter issues, you may need to:"
        print_warning "  1. Temporarily disable SELinux: sudo setenforce 0"
        print_warning "  2. Or create appropriate SELinux policies for debugging"
    fi
else
    print_status "SELinux is not installed"
fi

print_header "System Information"

# Display system information
print_status "Fedora version: $(cat /etc/fedora-release)"
print_status "Kernel version: $(uname -r)"
print_status "Architecture: $(uname -m)"
print_status "GDB version: $(gdb --version | head -1)"
print_status "Python version: $(python3 --version)"
print_status "GCC version: $(gcc --version | head -1)"

if command -v rustc &> /dev/null; then
    print_status "Rust version: $(rustc --version)"
fi

print_header "Verification"

# Verify installations
print_status "Verifying installations..."

VERIFICATION_FAILED=false

# Check critical tools
for tool in gdb python3 gcc make git curl; do
    if command -v $tool &> /dev/null; then
        print_status "✓ $tool is installed"
    else
        print_error "✗ $tool is missing"
        VERIFICATION_FAILED=true
    fi
done

# Check hatch
if command -v hatch &> /dev/null; then
    print_status "✓ hatch is installed"
elif [[ -f ~/.local/bin/hatch ]]; then
    print_status "✓ hatch is installed in ~/.local/bin"
else
    print_error "✗ hatch is missing"
    VERIFICATION_FAILED=true
fi

# Check Rust
if command -v rustc &> /dev/null; then
    print_status "✓ Rust is installed"
elif [[ -f ~/.cargo/bin/rustc ]]; then
    print_status "✓ Rust is installed in ~/.cargo/bin"
else
    print_error "✗ Rust is missing"
    VERIFICATION_FAILED=true
fi

print_header "Setup Complete"

if [[ "$VERIFICATION_FAILED" == "true" ]]; then
    print_error "Setup completed with errors. Please review the output above."
    exit 1
else
    print_status "All dependencies installed successfully!"
fi

echo
print_status "🎉 Fedora debug environment setup complete!"
echo
print_status "Next steps:"
print_status "1. Source your bashrc: source ~/.bashrc"
print_status "2. Clone the Snowflake CLI repository (if not already done):"
print_status "   git clone https://github.com/snowflakedb/snowflake-cli.git"
print_status "3. Navigate to the project directory: cd snowflake-cli"
print_status "4. Build the debug binary: hatch -e packaging run build-debug-binaries"
print_status "5. Debug with GDB: gdb ./dist/snow-debug/snow-debug"
echo
print_status "For more information, see DEBUG_BUILD.md"
echo
print_warning "Note: You may need to restart your shell or run 'source ~/.bashrc' for all changes to take effect."
