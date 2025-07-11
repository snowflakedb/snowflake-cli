#!/usr/bin/env bash

# Fedora Debug Build Deployment Script
# This script deploys the Snowflake CLI debug build to a remote Fedora system

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

# Usage function
usage() {
    echo "Usage: $0 [OPTIONS] USER@HOST"
    echo ""
    echo "Deploy Snowflake CLI debug build to a remote Fedora system"
    echo ""
    echo "OPTIONS:"
    echo "  -h, --help                 Show this help message"
    echo "  -p, --port PORT           SSH port (default: 22)"
    echo "  -k, --key KEY_FILE        SSH private key file"
    echo "  -d, --destination PATH    Remote destination path (default: ~/snowflake-cli-debug)"
    echo "  --setup-only              Only deploy setup files, don't build or transfer binary"
    echo "  --build-remote            Build on remote system instead of transferring binary"
    echo "  --no-setup                Don't run setup script on remote system"
    echo ""
    echo "Examples:"
    echo "  $0 user@fedora-server.example.com"
    echo "  $0 -p 2222 -k ~/.ssh/id_rsa user@192.168.1.100"
    echo "  $0 --build-remote --destination /opt/snowflake-cli user@fedora-server"
    echo ""
    exit 1
}

# Default values
SSH_PORT=22
SSH_KEY=""
DESTINATION="~/snowflake-cli-debug"
SETUP_ONLY=false
BUILD_REMOTE=false
NO_SETUP=false
REMOTE_HOST=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            ;;
        -p|--port)
            SSH_PORT="$2"
            shift 2
            ;;
        -k|--key)
            SSH_KEY="$2"
            shift 2
            ;;
        -d|--destination)
            DESTINATION="$2"
            shift 2
            ;;
        --setup-only)
            SETUP_ONLY=true
            shift
            ;;
        --build-remote)
            BUILD_REMOTE=true
            shift
            ;;
        --no-setup)
            NO_SETUP=true
            shift
            ;;
        -*)
            print_error "Unknown option: $1"
            usage
            ;;
        *)
            if [[ -z "$REMOTE_HOST" ]]; then
                REMOTE_HOST="$1"
            else
                print_error "Too many arguments: $1"
                usage
            fi
            shift
            ;;
    esac
done

# Validate required arguments
if [[ -z "$REMOTE_HOST" ]]; then
    print_error "Remote host is required"
    usage
fi

# Build SSH command
SSH_CMD="ssh -p $SSH_PORT"
SCP_CMD="scp -P $SSH_PORT"
if [[ -n "$SSH_KEY" ]]; then
    SSH_CMD="$SSH_CMD -i $SSH_KEY"
    SCP_CMD="$SCP_CMD -i $SSH_KEY"
fi

# Check if we're in the correct directory
if [[ ! -f "pyproject.toml" ]] || [[ ! -d "scripts/packaging" ]]; then
    print_error "This script must be run from the root of the Snowflake CLI repository"
    exit 1
fi

print_header "Deploying Snowflake CLI Debug Build to $REMOTE_HOST"

# Test SSH connection
print_status "Testing SSH connection..."
if ! $SSH_CMD $REMOTE_HOST "echo 'SSH connection successful'" >/dev/null 2>&1; then
    print_error "Failed to connect to $REMOTE_HOST"
    print_error "Please check your SSH configuration and try again"
    exit 1
fi
print_status "SSH connection successful"

# Create destination directory on remote system
print_status "Creating destination directory on remote system..."
$SSH_CMD $REMOTE_HOST "mkdir -p $DESTINATION"

# Deploy setup files
print_status "Deploying setup files..."
$SCP_CMD scripts/packaging/setup_fedora_debug.sh $REMOTE_HOST:$DESTINATION/
$SCP_CMD DEBUG_BUILD.md $REMOTE_HOST:$DESTINATION/

if [[ "$SETUP_ONLY" == "true" ]]; then
    print_status "Setup files deployed. Skipping binary build and transfer."
    print_status "To complete setup, run the following on the remote system:"
    print_status "  cd $DESTINATION && ./setup_fedora_debug.sh"
    exit 0
fi

if [[ "$BUILD_REMOTE" == "true" ]]; then
    print_status "Building on remote system..."

    # Deploy source code
    print_status "Deploying source code..."
    # Create a temporary archive of the source code
    tar -czf /tmp/snowflake-cli-source.tar.gz \
        --exclude=dist \
        --exclude=.git \
        --exclude=.hatch \
        --exclude=__pycache__ \
        --exclude=*.pyc \
        .

    $SCP_CMD /tmp/snowflake-cli-source.tar.gz $REMOTE_HOST:$DESTINATION/
    rm /tmp/snowflake-cli-source.tar.gz

    # Extract and build on remote system
    $SSH_CMD $REMOTE_HOST "cd $DESTINATION && tar -xzf snowflake-cli-source.tar.gz && rm snowflake-cli-source.tar.gz"

    if [[ "$NO_SETUP" == "false" ]]; then
        print_status "Running setup script on remote system..."
        $SSH_CMD $REMOTE_HOST "cd $DESTINATION && ./setup_fedora_debug.sh"
    fi

    print_status "Building debug binary on remote system..."
    $SSH_CMD $REMOTE_HOST "cd $DESTINATION && source ~/.bashrc && hatch -e packaging run build-debug-binaries"

else
    # Build locally first
    print_status "Building debug binary locally..."
    if [[ ! -f "dist/snow-debug/snow-debug" ]]; then
        print_status "Debug binary not found. Building..."
        hatch -e packaging run build-debug-binaries
    else
        print_status "Debug binary already exists. Using existing binary."
    fi

    # Transfer binary to remote system
    print_status "Transferring debug binary to remote system..."
    $SCP_CMD dist/snow-debug/snow-debug $REMOTE_HOST:$DESTINATION/

    if [[ "$NO_SETUP" == "false" ]]; then
        print_status "Running setup script on remote system..."
        $SSH_CMD $REMOTE_HOST "cd $DESTINATION && ./setup_fedora_debug.sh"
    fi
fi

print_header "Deployment Complete"

# Get remote system information
print_status "Remote system information:"
$SSH_CMD $REMOTE_HOST "uname -a"
$SSH_CMD $REMOTE_HOST "cat /etc/fedora-release 2>/dev/null || echo 'Fedora release information not available'"

print_status "🎉 Deployment successful!"
echo
print_status "Debug build deployed to: $REMOTE_HOST:$DESTINATION"
echo
print_status "To start debugging on the remote system:"
print_status "  ssh $REMOTE_HOST"
print_status "  cd $DESTINATION"
if [[ "$BUILD_REMOTE" == "true" ]]; then
    print_status "  gdb ./dist/snow-debug/snow-debug"
else
    print_status "  gdb ./snow-debug"
fi
echo
print_status "To run the debug binary:"
if [[ "$BUILD_REMOTE" == "true" ]]; then
    print_status "  ./dist/snow-debug/snow-debug --help"
else
    print_status "  ./snow-debug --help"
fi
echo
print_status "For more information, see DEBUG_BUILD.md on the remote system"
