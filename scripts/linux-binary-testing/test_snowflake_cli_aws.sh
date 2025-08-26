#!/bin/bash

# AWS Linux Binary Testing Script for Snowflake CLI Cross-Platform Compatibility
# Tests both x86_64 and ARM64 packages on multiple Linux distributions

set -e

# Script directory and configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/config"

# Configurable parameters (must be set via config file or command line)
KEYPAIR="your-keypair-name"
PACKAGE_DIR="/path/to/packages"
SECURITY_GROUP="your-security-group-id"
VPC_ID="your-vpc-id"
SUBNET_ID="your-subnet-id"
REGION="your-aws-region"

# Load configuration file if it exists
if [ -f "$CONFIG_FILE" ]; then
    echo "Loading configuration from: $CONFIG_FILE"
    source "$CONFIG_FILE"
else
    echo "No config file found at: $CONFIG_FILE"
    echo "Copy scripts/linux-binary-testing/config.example to scripts/linux-binary-testing/config and customize it for your environment"
fi

# Command line argument parsing
show_usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Required Options (must be provided via config file or command line):
    -k, --keypair KEYPAIR          AWS keypair name
    -p, --package-dir DIR          Directory containing packages
    -s, --security-group GROUP     AWS security group ID
    -v, --vpc-id VPC               AWS VPC ID
    -n, --subnet-id SUBNET         AWS subnet ID
    -r, --region REGION            AWS region

Other Options:
    -h, --help                     Show this help message

Configuration can also be set in the config file: $CONFIG_FILE
Command line arguments override config file values.

Example:
    $0 -k my-keypair -p /path/to/packages -s sg-12345 -v vpc-12345 -n subnet-12345 -r us-west-2

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -k|--keypair)
            KEYPAIR="$2"
            shift 2
            ;;
        -p|--package-dir)
            PACKAGE_DIR="$2"
            shift 2
            ;;
        -s|--security-group)
            SECURITY_GROUP="$2"
            shift 2
            ;;
        -v|--vpc-id)
            VPC_ID="$2"
            shift 2
            ;;
        -n|--subnet-id)
            SUBNET_ID="$2"
            shift 2
            ;;
        -r|--region)
            REGION="$2"
            shift 2
            ;;
        -h|--help)
            show_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
done

# Validate required configuration
validate_config() {
    local errors=0

    if [[ "$KEYPAIR" == "your-keypair-name" ]]; then
        echo "ERROR: KEYPAIR not configured"
        errors=1
    fi

    if [[ "$PACKAGE_DIR" == "/path/to/packages" ]]; then
        echo "ERROR: PACKAGE_DIR not configured"
        errors=1
    fi

    if [[ "$SECURITY_GROUP" == "your-security-group-id" ]]; then
        echo "ERROR: SECURITY_GROUP not configured"
        errors=1
    fi

    if [[ "$VPC_ID" == "your-vpc-id" ]]; then
        echo "ERROR: VPC_ID not configured"
        errors=1
    fi

    if [[ "$SUBNET_ID" == "your-subnet-id" ]]; then
        echo "ERROR: SUBNET_ID not configured"
        errors=1
    fi

    if [[ "$REGION" == "your-aws-region" ]]; then
        echo "ERROR: REGION not configured"
        errors=1
    fi

    if [ ! -d "$PACKAGE_DIR" ]; then
        echo "ERROR: Package directory not found: $PACKAGE_DIR"
        errors=1
    fi

    if [ $errors -eq 1 ]; then
        echo ""
        echo "Please configure the script by:"
        echo "1. Copying scripts/linux-binary-testing/config.example to scripts/linux-binary-testing/config and setting all required values"
        echo "2. Or using command line arguments to provide all required values"
        echo ""
        echo "Required configuration:"
        echo "  - KEYPAIR: AWS keypair name"
        echo "  - PACKAGE_DIR: Directory containing packages"
        echo "  - SECURITY_GROUP: AWS security group ID"
        echo "  - VPC_ID: AWS VPC ID"
        echo "  - SUBNET_ID: AWS subnet ID"
        echo "  - REGION: AWS region"
        echo ""
        show_usage
        exit 1
    fi
}

validate_config

# Create timestamp for this test run
TEST_RUN_TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
INSTANCE_TAG="snowflake-cli-test-$(whoami)-${TEST_RUN_TIMESTAMP}"

# Create dedicated logs directory for this run (relative to script location)
LOGS_DIR="${SCRIPT_DIR}/logs/${TEST_RUN_TIMESTAMP}"
mkdir -p "$LOGS_DIR"

echo "Test run timestamp: $TEST_RUN_TIMESTAMP"
echo "Logs will be saved to: $LOGS_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Global array to track launched instances (for cleanup from trap handler)
instances=()

# Global flag to control automatic cleanup (prevents race condition with user choice)
auto_cleanup_enabled=true

# Logging functions
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

success() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${RED}[ERROR]${NC} $1" >&2
}

warning() {
    echo -e "[$(date '+%Y-%m-%d %H:%M:%S')] ${YELLOW}[WARNING]${NC} $1"
}

# Define test configurations
# Format: "OS_NAME:AMI_ID:INSTANCE_TYPE:ARCH:PACKAGE_TYPE"
# Using verified AMIs for eu-central-1 region
CONFIGS=(
    # Amazon Linux 2023 AMI 2023.8.20250818.0 x86_64 HVM kernel-6.1
    "amazonlinux-x86:ami-015cbce10f839bd0c:t3.micro:x86_64:rpm"
    # Amazon Linux 2023 AMI 2023.8.20250818.0 arm64 HVM kernel-6.1
    "amazonlinux-arm:ami-0c76eed7d4d298d55:t4g.micro:arm64:rpm"

    # Red Hat Enterprise Linux 10 x86_64
    "redhat-10-x86:ami-005c89b47f40aa0e1:t3.micro:x86_64:rpm"
    # Red Hat Enterprise Linux 10 ARM64
    "redhat-10-arm:ami-0624b52bb7600a790:t4g.micro:arm64:rpm"

    # Red Hat Enterprise Linux 9 x86_64
    "redhat-9-x86:ami-0b6cae6b0598176ae:t3.micro:x86_64:rpm"
    # Red Hat Enterprise Linux 9 ARM64
    "redhat-9-arm:ami-091cd356fd6ad46ec:t4g.micro:arm64:rpm"

    # Debian x86_64
    "debian-x86:ami-0f439e819ba112bd7:t3.micro:x86_64:deb"
    # Debian ARM64
    "debian-arm:ami-0bdbe4d582d76c8ca:t4g.micro:arm64:deb"

    # Canonical, Ubuntu, 24.04, amd64 noble image
    "ubuntu-x86:ami-02003f9f0fde924ea:t3.micro:x86_64:deb"
    # Canonical, Ubuntu, 24.04, arm64 noble image
    "ubuntu-arm:ami-0fd8fe5cdf7cad6f6:t4g.micro:arm64:deb"
)

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    if ! command -v aws &> /dev/null; then
        error "AWS CLI not found. Please install it first."
        exit 1
    fi

    if ! aws sts get-caller-identity &> /dev/null; then
        error "AWS CLI not configured. Please run 'aws configure' first."
        exit 1
    fi

    if [ ! -d "$PACKAGE_DIR" ]; then
        error "Package directory $PACKAGE_DIR not found."
        exit 1
    fi

    success "Prerequisites check passed"
}

# Launch instance function
launch_instance() {
    local config="$1"
    IFS=':' read -r name ami instance_type arch package_type <<< "$config"

    log "Launching $name instance (AMI: $ami, Type: $instance_type, Arch: $arch)..." >&2

    local aws_output
    aws_output=$(aws ec2 run-instances \
        --image-id "$ami" \
        --count 1 \
        --instance-type "$instance_type" \
        --key-name "$KEYPAIR" \
        --security-group-ids "$SECURITY_GROUP" \
        --subnet-id "$SUBNET_ID" \
        --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$INSTANCE_TAG-$name},{Key=TestArch,Value=$arch},{Key=TestOS,Value=$name}]" \
        --region "$REGION" \
        --output text \
        --query 'Instances[0].InstanceId' 2>&1)
    local aws_result=$?

    local instance_id="$aws_output"

    if [ $aws_result -ne 0 ] || [ -z "$instance_id" ] || [ "$instance_id" = "None" ]; then
        echo "Failed to launch $name instance: $aws_output" >&2
        return 1
    fi

    success "Launched $name instance: $instance_id" >&2
    echo "$instance_id:$name:$arch:$package_type"
}

# Wait for instance to be ready
wait_for_instance() {
    local instance_id="$1"
    local name="$2"

    log "Waiting for $name instance ($instance_id) to be ready..."
    aws ec2 wait instance-running --instance-ids "$instance_id" --region "$REGION"
    sleep 30  # Wait for SSH to be available
    success "$name instance is ready"
}

# Get instance private IP
get_instance_ip() {
    local instance_id="$1"
    aws ec2 describe-instances \
        --instance-ids "$instance_id" \
        --region "$REGION" \
        --output text \
        --query 'Reservations[0].Instances[0].PrivateIpAddress'
}

# Generate test script content for remote execution
get_test_script_content() {
    local arch="$1"
    local package_type="$2"

    cat << EOF
#!/bin/bash

set -e

ARCH="$arch"
PACKAGE_TYPE="$package_type"

echo "=== Snowflake CLI Binary Test ==="
echo "OS: \$(cat /etc/os-release | grep PRETTY_NAME | cut -d'\"' -f2)"
echo "Architecture: \$ARCH"
echo ""

# Install the package
echo "Installing package..."

# Detect package file dynamically
if [ "\$ARCH" = "x86_64" ]; then
    ARCH_PATTERN="x86_64"
elif [ "\$ARCH" = "arm64" ]; then
    ARCH_PATTERN="aarch64"  # ARM64 packages use aarch64 in filename
else
    ARCH_PATTERN="\$ARCH"
fi

# Find package file by pattern
if [ "\$PACKAGE_TYPE" = "rpm" ]; then
    PACKAGE_FILE=\$(find . -name "snowflake-cli-*\${ARCH_PATTERN}.rpm" -type f | head -1)
else
    PACKAGE_FILE=\$(find . -name "snowflake-cli-*\${ARCH_PATTERN}.deb" -type f | head -1)
fi

if [ -z "\$PACKAGE_FILE" ]; then
    echo "ERROR: No \${PACKAGE_TYPE} package found for architecture \${ARCH_PATTERN}"
    echo "Available files:"
    ls -la ./*snowflake-cli* 2>/dev/null || echo "No snowflake-cli files found"
    exit 1
fi

echo "Detected package: \$PACKAGE_FILE"

# Install the package
if [ "\$PACKAGE_TYPE" = "rpm" ]; then
    if command -v yum &> /dev/null; then
        sudo yum install -y "./\$PACKAGE_FILE"
    elif command -v dnf &> /dev/null; then
        sudo dnf install -y "./\$PACKAGE_FILE"
    else
        sudo rpm -i "./\$PACKAGE_FILE"
    fi
elif [ "\$PACKAGE_TYPE" = "deb" ]; then
    sudo dpkg -i "./\$PACKAGE_FILE" || sudo apt-get install -f -y
fi

echo "Package installed successfully"
echo ""

# Test the binary
echo "Testing binary..."
if ! command -v snow &> /dev/null; then
    echo "ERROR: snow command not found"
    exit 1
fi

# Test basic functionality
echo "Running: snow --version"
if snow --version; then
    echo "✓ snow --version: SUCCESS"
else
    echo "✗ snow --version: FAILED"
    echo "Error details:"
    ldd \$(which snow) 2>&1 | head -10
    exit 1
fi

echo ""
echo "Running: snow --help"
if snow --help; then
    echo "✓ snow --help: SUCCESS"
else
    echo "✗ snow --help: FAILED"
    exit 1
fi

echo ""
echo "=== Test Result: SUCCESS ==="
EOF
}

# Test instance function
test_instance() {
    local instance_info="$1"
    IFS=':' read -r instance_id name arch package_type <<< "$instance_info"

    log "Testing $name instance ($instance_id)..."

    local ip=$(get_instance_ip "$instance_id")
    if [ -z "$ip" ]; then
        error "Could not get IP for $name instance"
        return 1
    fi

    log "$name instance private IP: $ip"

    # Detect package file dynamically
    local package_file
    local arch_pattern

    # Map architecture names for file detection
    if [ "$arch" = "x86_64" ]; then
        arch_pattern="x86_64"
    elif [ "$arch" = "arm64" ]; then
        arch_pattern="aarch64"  # ARM64 packages use aarch64 in filename
    else
        arch_pattern="$arch"
    fi

    # Find package file by pattern
    if [ "$package_type" = "rpm" ]; then
        package_file=$(find "$PACKAGE_DIR" -name "snowflake-cli-*${arch_pattern}.rpm" -type f | head -1)
    else
        package_file=$(find "$PACKAGE_DIR" -name "snowflake-cli-*${arch_pattern}.deb" -type f | head -1)
    fi

    # Check if package file was found
    if [ -z "$package_file" ]; then
        error "No ${package_type} package found for architecture ${arch_pattern} in $PACKAGE_DIR"
        error "Looking for pattern: snowflake-cli-*${arch_pattern}.${package_type}"
        return 1
    fi

    # Extract just the filename from the full path
    package_file=$(basename "$package_file")
    log "Detected package file: $package_file"

    # Copy files and run test
    local ssh_opts="-i ~/.ssh/${KEYPAIR}.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=30"
    local user="ec2-user"

    # Adjust username based on OS
    case "$name" in
        amazonlinux*) user="ec2-user" ;;
        redhat*) user="ec2-user" ;;
        ubuntu*) user="ubuntu" ;;
        debian*) user="admin" ;;
        suse*) user="ec2-user" ;;
        *) user="ec2-user" ;;
    esac

    # Wait for SSH to be ready with retries
    log "Waiting for SSH to be ready on $name instance..."
    local retry_count=0
    local max_retries=10

    while [ $retry_count -lt $max_retries ]; do
        if ssh $ssh_opts "$user@$ip" "echo 'SSH Ready'" &> /dev/null; then
            log "SSH connection established to $name instance"
            break
        else
            retry_count=$((retry_count + 1))
            log "SSH attempt $retry_count/$max_retries failed, waiting 30 seconds..."
            sleep 30
        fi
    done

    if [ $retry_count -eq $max_retries ]; then
        error "Failed to establish SSH connection to $name instance after $max_retries attempts"
        return 1
    fi

    log "Copying package to $name instance..."
    scp $ssh_opts "$PACKAGE_DIR/$package_file" "$user@$ip:~/" || {
        error "Failed to copy package to $name instance"
        return 1
    }

    log "Running test on $name instance..."
    # Send test script directly via SSH and execute
    get_test_script_content "$arch" "$package_type" | ssh $ssh_opts "$user@$ip" "cat > ~/test_script.sh && chmod +x ~/test_script.sh && ~/test_script.sh" > "$LOGS_DIR/test_results_${name}.log" 2>&1 || {
        error "Test failed on $name instance"
        cat "$LOGS_DIR/test_results_${name}.log"
        return 1
    }

    success "Test completed on $name instance"
    log "Results saved to $LOGS_DIR/test_results_${name}.log"

    # Show key results
    echo ""
    echo "=== Results for $name ($arch) ==="
    grep -E "(SUCCESS|FAILED|ERROR)" "$LOGS_DIR/test_results_${name}.log" || true
    echo ""
}

# Cleanup function
cleanup_instances() {
    # Respect user's choice - don't cleanup if explicitly disabled
    if [ "$auto_cleanup_enabled" = false ]; then
        return 0
    fi

    if [ ${#instances[@]} -eq 0 ]; then
        return 0
    fi

    log "Cleaning up test instances..."

    local instance_ids=()
    for instance_info in "${instances[@]}"; do
        IFS=':' read -r instance_id name arch package_type <<< "$instance_info"
        instance_ids+=("$instance_id")
    done

    if [ ${#instance_ids[@]} -gt 0 ]; then
        aws ec2 terminate-instances --instance-ids "${instance_ids[@]}" --region "$REGION" &> /dev/null
        success "Terminated instances: ${instance_ids[*]}"
    fi
}

# Main function
main() {
    check_prerequisites

    instances=()
    auto_cleanup_enabled=true
    local failed_launches=()
    local failed_tests=()
    local successful_tests=()

    log "This will launch ${#CONFIGS[@]} EC2 instances for testing."
    log "Launching instances..."

    # Launch all instances
    for config in "${CONFIGS[@]}"; do
        log "Processing config: $config"
        if instance_info=$(launch_instance "$config"); then
            instances+=("$instance_info")
            success "Successfully queued: $instance_info"
        else
            IFS=':' read -r name ami instance_type arch package_type <<< "$config"
            warning "Failed to launch instance for config: $config"
            failed_launches+=("$name")
        fi
    done

    if [ ${#instances[@]} -eq 0 ]; then
        error "No instances were launched successfully"
        exit 1
    fi

    log "Launched ${#instances[@]} instances, waiting for them to be ready..."

    # Wait for all instances to be ready
    for instance_info in "${instances[@]}"; do
        IFS=':' read -r instance_id name arch package_type <<< "$instance_info"
        wait_for_instance "$instance_id" "$name"
    done

    log "All instances are ready, starting tests..."

    # Test all instances
    for instance_info in "${instances[@]}"; do
        IFS=':' read -r instance_id name arch package_type <<< "$instance_info"
        if test_instance "$instance_info"; then
            successful_tests+=("$name")
        else
            failed_tests+=("$name")
        fi
    done

    # Summary
    echo ""
    echo "=== TEST SUMMARY ==="
    echo "Successful tests: ${#successful_tests[@]}"
    for test in "${successful_tests[@]}"; do
        echo "  ✓ $test"
    done

    echo ""
    echo "Failed tests: ${#failed_tests[@]}"
    for test in "${failed_tests[@]}"; do
        echo "  ✗ $test"
    done

    echo ""
    echo "Failed launches: ${#failed_launches[@]}"
    for launch in "${failed_launches[@]}"; do
        echo "  ✗ $launch"
    done

    # Cleanup
    read -p "Clean up instances now? (Y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        cleanup_instances
    else
        # User chose not to cleanup - disable automatic cleanup to prevent race condition
        auto_cleanup_enabled=false
        warning "Instances left running. Clean up manually to avoid charges:"
        warning "aws ec2 terminate-instances --instance-ids \$(aws ec2 describe-instances --region $REGION --filters \"Name=tag:Name,Values=$INSTANCE_TAG-*\" \"Name=instance-state-name,Values=running\" --output text --query 'Reservations[].Instances[].InstanceId')"
    fi

    log "Testing completed!"
    log "All test scripts and results saved in: $LOGS_DIR"
    log "Test run timestamp: $TEST_RUN_TIMESTAMP"
}

# Trap for cleanup on script exit
trap 'cleanup_instances' EXIT

# Run main function
main "$@"
