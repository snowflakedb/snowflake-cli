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

# Fedora RPM Debug Package Guide

This guide explains how to build and debug RPM packages for the Snowflake CLI on Fedora systems.

## 🎯 Overview

Building an RPM package provides several advantages for debugging on Fedora:
- **System Integration**: Installs cleanly with proper dependencies
- **Debug Info Packages**: Separate debuginfo RPMs for optimal debugging
- **Standard Locations**: Files installed in standard system locations
- **Package Management**: Easy installation, removal, and updates
- **Distribution Ready**: Can be shared and deployed across Fedora systems

## 📋 Prerequisites

### System Requirements
- **Fedora 35+** (or RHEL/CentOS 8+)
- **4GB+ RAM** (or 2GB+ with swap space)
- **5GB+ free disk space**
- **Internet connection** for downloading dependencies

### Required Packages
```bash
# Install RPM build tools
sudo dnf install rpm-build rpmdevtools

# Install build dependencies
sudo dnf install python3-devel gcc gcc-c++ rust cargo gdb git

# Install development tools (optional but recommended)
sudo dnf groupinstall "Development Tools"
```

## 🚀 Building the Debug RPM

### Quick Build
```bash
# Build the debug RPM package
hatch -e packaging run build-debug-rpm
```

### Manual Build Process

#### 1. Setup RPM Build Environment
```bash
# Create RPM build directories
rpmdev-setuptree

# Verify structure
ls ~/rpmbuild
# Should show: BUILD  BUILDROOT  RPMS  SOURCES  SPECS  SRPMS
```

#### 2. Build the Package
```bash
# Navigate to project root
cd /path/to/snowflake-cli

# Run the build script
./scripts/packaging/build_debug_rpm.sh
```

#### 3. Monitor the Build
The build process will:
- Create source tarball
- Install build dependencies
- Compile with debug symbols
- Generate main and debuginfo RPMs
- Provide installation instructions

## 📦 Package Details

### Main Package: `snowflake-cli-debug`
- **Binary**: `/usr/bin/snow-debug`
- **Documentation**: `/usr/share/doc/snowflake-cli-debug/`
- **Man Page**: `/usr/share/man/man1/snow-debug.1`

### Debug Info Package: `snowflake-cli-debug-debuginfo`
- **Debug Symbols**: `/usr/lib/debug/usr/bin/snow-debug.debug`
- **Source Code**: `/usr/src/debug/snowflake-cli-debug-*/`

## 🔧 Installation

### Install Main Package
```bash
# Install the main debug package
sudo dnf install ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug-*.rpm
```

### Install with Debug Symbols
```bash
# Install both main and debuginfo packages
sudo dnf install ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug-*.rpm \
                 ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug-debuginfo-*.rpm
```

### Verify Installation
```bash
# Check installed files
rpm -ql snowflake-cli-debug

# Verify binary works
snow-debug --help

# Check debug symbols
file /usr/bin/snow-debug
objdump -h /usr/bin/snow-debug | grep debug
```

## 🐛 Debugging the RPM Package

### Basic GDB Usage
```bash
# Start debugging
gdb /usr/bin/snow-debug

# Basic GDB commands
(gdb) run --help
(gdb) break main
(gdb) run connection list
(gdb) backtrace
(gdb) info locals
```

### With Debug Symbols Installed
```bash
# GDB will automatically load debug symbols
gdb /usr/bin/snow-debug

# Enhanced debugging capabilities
(gdb) list                    # Show source code
(gdb) break filename.py:123   # Set breakpoints by line
(gdb) info variables          # Show variables
(gdb) print variable_name     # Print variable values
```

### Python-Specific Debugging
```bash
# If Python debugging symbols are available
gdb /usr/bin/snow-debug

# Python debugging commands
(gdb) py-bt                   # Python backtrace
(gdb) py-list                 # Show Python source
(gdb) py-locals               # Show Python variables
(gdb) py-up / py-down         # Navigate Python stack
```

### Core Dump Analysis
```bash
# Enable core dumps
echo 'kernel.core_pattern=/tmp/core.%e.%p.%h.%t' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
ulimit -c unlimited

# Generate core dump (if application crashes)
snow-debug problematic-command

# Analyze core dump
gdb /usr/bin/snow-debug /tmp/core.*
(gdb) backtrace
(gdb) info registers
```

### Remote Debugging
```bash
# On target system: start gdbserver
gdbserver :2345 /usr/bin/snow-debug --help

# On development system: connect remotely
gdb /usr/bin/snow-debug
(gdb) target remote target-system:2345
```

## 🛠️ Advanced Debugging

### Debugging with Systemd
```bash
# If running as a service, attach to running process
sudo gdb -p $(pgrep snow-debug)

# Or debug through systemd
sudo systemd-run --uid=1000 --gid=1000 gdb /usr/bin/snow-debug
```

### Memory Debugging with Valgrind
```bash
# Install valgrind
sudo dnf install valgrind

# Run with memory checking
valgrind --tool=memcheck --leak-check=full /usr/bin/snow-debug --help
```

### Performance Profiling
```bash
# Install perf tools
sudo dnf install perf

# Profile the application
perf record /usr/bin/snow-debug command
perf report
```

## 🔍 Troubleshooting

### Build Issues

#### Out of Memory During Build
```bash
# Add swap space
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Reduce build parallelism
export CARGO_BUILD_JOBS=1
```

#### Missing Dependencies
```bash
# Install build dependencies automatically
sudo dnf builddep ~/rpmbuild/SPECS/snowflake-cli-debug.spec

# Or install manually
sudo dnf install python3-devel gcc gcc-c++ rust cargo
```

#### Rust Compilation Issues
```bash
# Update Rust toolchain
rustup update stable

# Clean cargo cache
cargo clean
rm -rf ~/.cargo/registry/cache
```

### Runtime Issues

#### Debug Symbols Not Loading
```bash
# Check if debuginfo package is installed
rpm -q snowflake-cli-debug-debuginfo

# Verify debug symbols location
ls -la /usr/lib/debug/usr/bin/

# Force GDB to load symbols
gdb /usr/bin/snow-debug
(gdb) symbol-file /usr/lib/debug/usr/bin/snow-debug.debug
```

#### Python Debugging Not Working
```bash
# Install Python debug packages
sudo dnf install python3-debuginfo python3-debug

# Check GDB Python extensions
ls /usr/share/gdb/auto-load/usr/bin/python*

# Load Python debugging extensions manually
(gdb) source /usr/share/gdb/auto-load/usr/bin/python3.*-gdb.py
```

### SELinux Issues
```bash
# Check SELinux context
ls -Z /usr/bin/snow-debug

# If needed, restore SELinux context
sudo restorecon -v /usr/bin/snow-debug

# Temporarily disable SELinux for debugging
sudo setenforce 0
```

## 📊 Package Management

### Updating the Package
```bash
# Build new version
hatch -e packaging run build-debug-rpm

# Update installation
sudo dnf update ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug-*.rpm
```

### Removing the Package
```bash
# Remove the package
sudo dnf remove snowflake-cli-debug snowflake-cli-debug-debuginfo

# Clean up build files
rm -rf ~/rpmbuild
```

### Creating a Repository
```bash
# Create a local repository
mkdir -p ~/snow-repo
cp ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug*.rpm ~/snow-repo/
createrepo ~/snow-repo

# Add to DNF configuration
echo "[snow-debug]
name=Snowflake CLI Debug Repository
baseurl=file:///home/$USER/snow-repo
enabled=1
gpgcheck=0" | sudo tee /etc/yum.repos.d/snow-debug.repo

# Install from repository
sudo dnf install snowflake-cli-debug
```

## 🎓 Best Practices

### Development Workflow
1. **Development**: Use standalone debug binary for active development
2. **Testing**: Create RPM for integration testing
3. **Deployment**: Use RPM for production debugging environments
4. **Distribution**: Share RPMs across team/organization

### Security Considerations
- Debug packages should not be used in production
- Remove debug symbols from production deployments
- Use separate debug environments
- Limit access to debug builds

### Performance Considerations
- Debug builds are slower than optimized builds
- Debug symbols increase disk usage
- Use debug builds only when needed
- Monitor system resources during debugging

## 📚 Additional Resources

### Documentation
- **RPM Packaging Guide**: `/usr/share/doc/snowflake-cli-debug/`
- **GDB Manual**: `info gdb`
- **Fedora Packaging Guidelines**: https://docs.fedoraproject.org/en-US/packaging-guidelines/

### Tools
- **GDB**: Advanced debugging
- **Valgrind**: Memory debugging
- **perf**: Performance profiling
- **strace**: System call tracing

## 🆘 Getting Help

### Check Package Status
```bash
# Package information
rpm -qi snowflake-cli-debug

# List package files
rpm -ql snowflake-cli-debug

# Verify package integrity
rpm -V snowflake-cli-debug
```

### Debug Information
```bash
# System information
uname -a
cat /etc/fedora-release

# Build environment
rpmbuild --showrc | grep -E "(build|debug)"

# Debug symbols
objdump -h /usr/bin/snow-debug | grep debug
```

This guide provides comprehensive instructions for building, installing, and debugging RPM packages of the Snowflake CLI on Fedora systems. The RPM approach offers professional package management and enhanced debugging capabilities for enterprise environments.
