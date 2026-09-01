<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# Linux Binary Testing Suite

Automated cross-platform compatibility testing for Snowflake CLI binaries across multiple Linux distributions and architectures.

## 🎯 Purpose

This testing suite validates that Snowflake CLI packages work correctly on various Linux distributions by:
- Installing the package on fresh instances
- Testing basic CLI functionality (`snow --version`, `snow --help`)
- Providing clear **SUCCESS/FAILED** results for each platform

## 🏗️ Test Coverage

The suite tests **10 Linux distributions** across **2 architectures**:

### Distributions
- **Amazon Linux 2023** (x86_64 + ARM64)
- **Red Hat Enterprise Linux 9** (x86_64 + ARM64)
- **Red Hat Enterprise Linux 10** (x86_64 + ARM64)
- **Debian 13** (x86_64 + ARM64)
- **Ubuntu 24.04 LTS** (x86_64 + ARM64)

### Package Types
- **RPM packages**: Amazon Linux, Red Hat
- **DEB packages**: Debian, Ubuntu

## 🚀 Quick Start

### 1. Setup Configuration
```bash
# Copy the example config
cp scripts/linux-binary-testing/config.example scripts/linux-binary-testing/config

# Edit with your settings
vim scripts/linux-binary-testing/config
```

Required configuration:
```bash
# Basic configuration
KEYPAIR="your-aws-keypair-name"
PACKAGE_DIR="/path/to/your/snowflake-cli/packages"

# AWS environment configuration
SECURITY_GROUP="your-security-group-id"
VPC_ID="your-vpc-id"
SUBNET_ID="your-subnet-id"
REGION="your-aws-region"
```

### 2. Build Packages

The test script requires pre-built `.rpm` and `.deb` packages. These must be
built inside Docker (the binaries target Linux and depend on a custom Python
built in the container).

```bash
cd scripts/packaging

# Build the Docker image (once, or after Dockerfile changes)
docker compose build

# Build x86_64 packages
DOCKER_DEFAULT_PLATFORM=linux/amd64 docker compose run --rm package-builder \
  bash -c "scripts/packaging/build_binaries.sh && scripts/packaging/build_packages.sh"

# Build aarch64 packages
DOCKER_DEFAULT_PLATFORM=linux/arm64 docker compose run --rm package-builder \
  bash -c "scripts/packaging/build_binaries.sh && scripts/packaging/build_packages.sh"
```

Packages are written to `dist/` in the repo root:
```
dist/
├── snowflake-cli-<version>.x86_64.rpm
├── snowflake-cli-<version>.x86_64.deb
├── snowflake-cli-<version>.aarch64.rpm
└── snowflake-cli-<version>.aarch64.deb
```

Set `PACKAGE_DIR` to the `dist/` directory when running the tests.

### 3. Run Tests
```bash
# Run the test suite (using config file)
./scripts/linux-binary-testing/test_snowflake_cli_aws.sh

# Or with command line arguments (all required)
./scripts/linux-binary-testing/test_snowflake_cli_aws.sh \
  -k my-keypair \
  -p /path/to/packages \
  -s sg-abcde \
  -v vpc-abcde \
  -n subnet-abcde \
  -r region_name
```

### 4. View Results
```bash
# Test results are saved in timestamped directories
ls scripts/linux-binary-testing/logs/

# Example output location
scripts/linux-binary-testing/logs/20240821_143000/
├── test_results_amazonlinux-x86.log
├── test_results_amazonlinux-arm.log
├── test_results_ubuntu-x86.log
└── ...
```

## 📋 Prerequisites

### AWS Setup
- **AWS CLI** installed and configured (`aws configure`)
- **EC2 permissions** for launching instances (RunInstances, DescribeInstances, TerminateInstances)
- **SSH key pair** created in AWS (for connecting to instances)
- **VPC with subnet** configured for instance deployment
- **Security Group** allowing SSH access (port 22) from your IP
- **AWS region** where resources are available

### Local Requirements
- **Bash** shell
- **SSH client**
- **Snowflake CLI packages** built and ready for testing

### Package Structure Expected
The script automatically detects package files with this naming pattern:
```
snowflake-cli-*x86_64.rpm     # x86_64 RPM packages
snowflake-cli-*aarch64.rpm    # ARM64 RPM packages
snowflake-cli-*x86_64.deb     # x86_64 DEB packages
snowflake-cli-*aarch64.deb    # ARM64 DEB packages
```

Examples:
- `snowflake-cli-3.12.0.dev0.x86_64.rpm`
- `snowflake-cli-4.0.0.aarch64.deb`

## 📊 Example Output

```
=== TEST SUMMARY ===
Successful tests: 8
  ✓ amazonlinux-x86
  ✓ redhat-9-x86
  ✓ ubuntu-x86
  ✓ ubuntu-arm
  ✓ debian-x86
  ✓ debian-arm
  ✓ redhat-10-x86
  ✓ redhat-10-arm

Failed tests: 2
  ✗ amazonlinux-arm
  ✗ redhat-9-arm

Failed launches: 0
```

## 🎛️ Configuration Options

### Command Line Arguments
Required options (must be provided via config file or command line):
```bash
-k, --keypair KEYPAIR          AWS keypair name
-p, --package-dir DIR          Directory containing packages
-s, --security-group GROUP     AWS security group ID
-v, --vpc-id VPC               AWS VPC ID
-n, --subnet-id SUBNET         AWS subnet ID
-r, --region REGION            AWS region
```

Other options:
```bash
-h, --help                     Show help message
```

### Config File
```bash
# scripts/linux-binary-testing/config

# Basic configuration
KEYPAIR="my-aws-keypair"
PACKAGE_DIR="/Users/username/snowflake-cli/packages"

# AWS environment configuration
SECURITY_GROUP="sg-abcde"
VPC_ID="vpc-abcde"
SUBNET_ID="subnet-abcde"
REGION="region_name"
```

## 🔧 How It Works

1. **Launch Instances**: Spins up EC2 instances for each Linux distribution
2. **Wait for Ready**: Waits for instances to boot and SSH to be available
3. **Copy Packages**: Transfers the appropriate package to each instance
4. **Install & Test**: Installs package and runs basic CLI tests
5. **Collect Results**: Saves test output and provides summary
6. **Cleanup**: Terminates instances (with user confirmation)

## 📝 Test Process Per Instance

Each instance runs this simple test:
```bash
# 1. Detect and install package
Installing package...
Detected package: ./snowflake-cli-3.12.0.dev0.x86_64.rpm
Package installed successfully

# 2. Test basic functionality
Testing binary...
Running: snow --version
Snowflake CLI version: 3.12.0.dev0
✓ snow --version: SUCCESS

Running: snow --help
Usage: -c [OPTIONS] COMMAND [ARGS]...

Snowflake CLI tool for developers [v3.12.0.dev0]

╭─ Options ────────────────────────────────────────────────────────────────────╮
│ --help                -h            Show this message and exit.              │
│ --version                           Shows version of the Snowflake CLI       │
[... full help output ...]
╰──────────────────────────────────────────────────────────────────────────────╯
✓ snow --help: SUCCESS

=== Test Result: SUCCESS ===
```

## 🗂️ Directory Structure

```
scripts/linux-binary-testing/
├── README.md                    # This file
├── test_snowflake_cli_aws.sh   # Main test script
├── config.example              # Configuration template
├── config                      # Your personal config (ignored by Git)
└── logs/                       # Test results (ignored by Git)
    └── 20240821_143000/        # Timestamped test run
        ├── test_results_amazonlinux-x86.log
        ├── test_results_ubuntu-arm.log
        └── ...
```

## 💡 Tips

- **Version Agnostic**: Works with any Snowflake CLI version
- **Parallel Testing**: All instances run simultaneously for faster results
- **Complete Logs**: Captures full CLI output including help text for verification
- **Safe Cleanup**: Always confirms before terminating instances
- **SSH Retry**: Automatically retries SSH connections with backoff

## 🐛 Troubleshooting

### Common Issues

**Configuration not set:**
```bash
ERROR: SECURITY_GROUP not configured
ERROR: VPC_ID not configured
ERROR: SUBNET_ID not configured
ERROR: REGION not configured
```
→ Set all required AWS configuration in config file or command line arguments

**Package not found:**
```bash
ERROR: No rpm package found for architecture x86_64 in /path/to/packages
Looking for pattern: snowflake-cli-*x86_64.rpm
```
→ Check package directory path and filename patterns

**SSH connection failed:**
```bash
ERROR: Failed to establish SSH connection after 10 attempts
```
→ Verify security group allows SSH (port 22) and keypair is correct

**AWS permissions:**
```bash
ERROR: AWS CLI not configured
```
→ Run `aws configure` or check IAM permissions for EC2

**Invalid AWS resources:**
```bash
ERROR: Could not get IP for instance
```
→ Verify VPC, subnet, and security group IDs exist in the specified region

### Getting Help

- Check logs in `scripts/linux-binary-testing/logs/[timestamp]/`
- Verify AWS credentials: `aws sts get-caller-identity`
- Verify AWS resources exist: `aws ec2 describe-vpcs --vpc-ids your-vpc-id --region your-region`
- Test SSH manually: `ssh -i ~/.ssh/your-key.pem ec2-user@instance-ip`
- Check script configuration: `./test_snowflake_cli_aws.sh --help`

## 🔒 Security

- **Private instances**: Uses private IPs within VPC
- **SSH keys**: Requires your AWS keypair for access
- **Auto-cleanup**: Terminates instances after testing
- **No persistence**: Fresh instances each run

---

**Ready to test?** Configure your settings and run `./test_snowflake_cli_aws.sh`! 🚀
