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

# Fedora Quick Start Guide - Debug Build

This guide helps you quickly deploy and run the Snowflake CLI debug build on a remote Fedora system.

## ⚠️ Important: Architecture Compatibility

**The debug binary must be built on the same architecture as your target system.** If you built on macOS/ARM64 but are deploying to Linux/x86_64, you'll get "not in executable format" errors.

**Recommended Solution:** Use `--build-remote` to build directly on your Fedora system.

## 🚀 Quick Deployment

### Option 1: Build on Remote System (⭐ Recommended)

```bash
# Deploy source code and build on the remote system
./scripts/packaging/deploy_fedora_debug.sh --build-remote user@your-fedora-server.com

# SSH to your server and start debugging
ssh user@your-fedora-server.com
cd ~/snowflake-cli-debug
gdb ./dist/snow-debug/snow-debug
```

### Option 2: RPM Package (Professional/Enterprise)

```bash
# Build debug RPM package
hatch -e packaging run build-debug-rpm

# Install the RPM package
sudo dnf install ~/rpmbuild/RPMS/x86_64/snowflake-cli-debug-*.rpm

# Debug the installed package
gdb /usr/bin/snow-debug
```

For complete RPM packaging instructions, see **[FEDORA_RPM_DEBUG.md](FEDORA_RPM_DEBUG.md)**.

### Option 3: Deploy Pre-built Binary (Only if architectures match)

```bash
# Only use this if your local machine and remote system have the same architecture
./scripts/packaging/deploy_fedora_debug.sh user@your-fedora-server.com

# SSH to your server and start debugging
ssh user@your-fedora-server.com
cd ~/snowflake-cli-debug
gdb ./snow-debug
```

### Option 4: Manual Setup

```bash
# Deploy only the setup files
./scripts/packaging/deploy_fedora_debug.sh --setup-only user@your-fedora-server.com

# SSH to your server and complete setup manually
ssh user@your-fedora-server.com
cd ~/snowflake-cli-debug
./setup_fedora_debug.sh

# Clone and build the project
git clone https://github.com/snowflakedb/snowflake-cli.git
cd snowflake-cli
hatch -e packaging run build-debug-binaries
```

## 📋 Prerequisites

### On Your Local Machine
- SSH access to the remote Fedora system
- This repository cloned locally
- Basic build tools (for Option 1)

### On Remote Fedora System
- Fedora 35+ (tested)
- Sudo access for installing packages
- Internet connection for downloading dependencies

## 🔧 Deployment Options

### Basic Deployment
```bash
./scripts/packaging/deploy_fedora_debug.sh user@fedora-server.com
```

### With Custom SSH Port
```bash
./scripts/packaging/deploy_fedora_debug.sh -p 2222 user@fedora-server.com
```

### With SSH Key
```bash
./scripts/packaging/deploy_fedora_debug.sh -k ~/.ssh/id_rsa user@fedora-server.com
```

### Custom Destination
```bash
./scripts/packaging/deploy_fedora_debug.sh -d /opt/snowflake-cli user@fedora-server.com
```

### Build on Remote System
```bash
./scripts/packaging/deploy_fedora_debug.sh --build-remote user@fedora-server.com
```

## 🐛 Using GDB on Fedora

Once deployed, you can debug the CLI:

### Basic GDB Usage
```bash
# Start GDB with the debug binary
gdb ./snow-debug

# Set breakpoints and run
(gdb) break main
(gdb) run --help

# Examine the stack when stopped
(gdb) backtrace
(gdb) info locals
```

### Python-Specific Debugging
```bash
# If Python debugging symbols are available
(gdb) py-bt        # Python backtrace
(gdb) py-list      # Show Python source
(gdb) py-locals    # Show Python variables
```

### Remote GDB Debugging
```bash
# On the Fedora system, start gdbserver
gdbserver :2345 ./snow-debug --help

# From your local machine
gdb ./dist/snow-debug/snow-debug
(gdb) target remote fedora-server.com:2345
```

## 🛠️ Troubleshooting

### Disk Space Issues During Build

**Error:** `No space left on device (os error 28)` during Rust/Cargo build

**Cause:** The build process requires significant temporary space for Rust compilation.

**Solutions:**

#### 1. Check Available Disk Space
```bash
# Check overall disk usage
df -h

# Check /tmp directory specifically
df -h /tmp

# Check available space in home directory
df -h ~
```

#### 2. Clean Up Temporary Files
```bash
# Clean system temporary files
sudo rm -rf /tmp/cargo-install*
sudo rm -rf /tmp/rust*

# Clean user cargo cache
rm -rf ~/.cargo/registry/cache
rm -rf ~/.cargo/registry/src
rm -rf ~/.cargo/git

# Clean hatch environments (if space is needed)
rm -rf ~/.local/share/hatch/env
```

#### 3. Use Alternative Temporary Directory
```bash
# Create a temporary directory in your home folder (usually has more space)
mkdir -p ~/tmp

# Set TMPDIR environment variable
export TMPDIR=~/tmp
export CARGO_TARGET_DIR=~/tmp/cargo-target

# Then retry the build
cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries
```

#### 4. Free Up System Space
```bash
# Remove old kernels (Fedora)
sudo dnf remove $(dnf repoquery --installonly --latest-limit=-2 -q)

# Clean package cache
sudo dnf clean all

# Remove old logs
sudo journalctl --vacuum-time=2weeks

# Clean user cache
rm -rf ~/.cache/*
```

#### 5. Build with Limited Parallelism
```bash
# Reduce parallel jobs to use less temporary space
export CARGO_BUILD_JOBS=1

# Then retry the build
cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries
```

#### 6. Alternative: Build in Steps
```bash
# If still having issues, try building Rust separately first
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Clean and retry
cargo clean
cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries
```

### Memory Issues During Build (OOM Killer)

**Error:** `process didn't exit successfully` with `(signal: 9, SIGKILL: kill)` during Rust compilation

**Cause:** The system ran out of memory and the OOM killer terminated the compiler process. Debug builds with full optimization are very memory-intensive.

**Solutions:**

#### 1. Check Memory Usage
```bash
# Check available memory
free -h

# Check current memory usage
top

# Monitor memory during build (in another terminal)
watch free -h
```

#### 2. Add Swap Space (if none exists)
```bash
# Check current swap
sudo swapon --show

# Create swap file if none exists (2GB example)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Make permanent (optional)
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

#### 3. Reduce Compilation Memory Usage
```bash
# Set environment variables to reduce memory usage
export CARGO_BUILD_JOBS=1              # Single-threaded compilation
export RUSTFLAGS="-C opt-level=1"      # Reduce optimization level
export PYAPP_DISTRIBUTION_EMBED=1      # Reduce PyApp memory usage

# Alternative: disable LTO to save memory
export RUSTFLAGS="-C opt-level=1 -C lto=off"

# Then retry the build
cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries
```

#### 4. Use Release Mode with Debug Symbols
```bash
# Build with less aggressive optimization but keep debug symbols
export RUSTFLAGS="-C opt-level=1 -C debuginfo=2 -C lto=off"
export CARGO_BUILD_JOBS=1

cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries
```

#### 5. Close Other Applications
```bash
# Free up memory by closing unnecessary applications
# Check what's using memory
ps aux --sort=-%mem | head -10

# Kill unnecessary processes if needed
```

#### 6. Alternative: Build on a Machine with More RAM
If your Fedora system has limited RAM (< 4GB), consider:
- Building on a machine with more memory
- Using a cloud instance with more RAM temporarily
- Building in a container with memory limits adjusted

#### 7. Monitor System Resources During Build
```bash
# In one terminal, start the build
cd ~/snowflake-cli-debug
hatch -e packaging run build-debug-binaries

# In another terminal, monitor resources
watch 'free -h && echo "--- CPU ---" && top -bn1 | head -5'
```

### Architecture Mismatch Error

**Error:** `"not in executable format: file format not recognized"`

**Cause:** The binary was built on a different architecture (e.g., macOS ARM64) than your Fedora system (e.g., Linux x86_64).

**Solution:**
```bash
# Use build-remote to build on the target system
./scripts/packaging/deploy_fedora_debug.sh --build-remote user@fedora-server.com

# Or build manually on the Fedora system
ssh user@fedora-server.com
cd ~/snowflake-cli-debug
git clone https://github.com/snowflakedb/snowflake-cli.git
cd snowflake-cli
hatch -e packaging run build-debug-binaries
```

**Check architectures:**
```bash
# Check your local architecture
uname -m

# Check remote system architecture
ssh user@fedora-server.com "uname -m"

# Check binary architecture
ssh user@fedora-server.com "file ~/snowflake-cli-debug/snow-debug"
```

### SSH Connection Issues
```bash
# Test SSH connection
ssh user@fedora-server.com "echo 'Connection successful'"

# Check SSH configuration
ssh -v user@fedora-server.com
```

### Permission Issues
```bash
# Ensure you have sudo access on the remote system
ssh user@fedora-server.com "sudo echo 'Sudo access confirmed'"
```

### SELinux Issues
```bash
# Check SELinux status
ssh user@fedora-server.com "getenforce"

# Temporarily disable if needed (for debugging only)
ssh user@fedora-server.com "sudo setenforce 0"
```

### Build Failures
```bash
# Check system requirements
ssh user@fedora-server.com "dnf list installed | grep -E '(gcc|python3-devel|gdb)'"

# Manually install missing dependencies
ssh user@fedora-server.com "sudo dnf install -y gcc python3-devel gdb"
```

## 📚 Environment Variables

The debug build supports these environment variables:

```bash
# Enable full Rust backtraces
export RUST_BACKTRACE=full

# Enable Python development mode
export PYTHONDEVMODE=1

# Enable CLI debug logging
export SNOWFLAKE_CLI_DEBUG=1
```

## 🎯 Common Debug Scenarios

### Debugging CLI Commands
```bash
# Debug a specific command
gdb ./snow-debug
(gdb) run connection list --help
```

### Debugging Crashes
```bash
# Enable core dumps
ulimit -c unlimited

# Run the command that crashes
./snow-debug problematic-command

# Debug the core dump
gdb ./snow-debug core.*
```

### Debugging Hang/Freeze
```bash
# Attach to running process
./snow-debug command-that-hangs &
PID=$!
gdb -p $PID
```

## 📞 Getting Help

### Check Setup Status
```bash
# Verify installation
ssh user@fedora-server.com "cd ~/snowflake-cli-debug && ./setup_fedora_debug.sh --help"

# Check debug binary
ssh user@fedora-server.com "file ~/snowflake-cli-debug/snow-debug"
```

### View Documentation
```bash
# View the full debug documentation
ssh user@fedora-server.com "cd ~/snowflake-cli-debug && cat DEBUG_BUILD.md"
```

### System Information
```bash
# Get system details
ssh user@fedora-server.com "uname -a && cat /etc/fedora-release"
```

## 🔄 Updating the Debug Build

To update the debug build on your remote system:

```bash
# Rebuild and redeploy
./scripts/packaging/deploy_fedora_debug.sh user@fedora-server.com

# Or rebuild on remote system
ssh user@fedora-server.com "cd ~/snowflake-cli-debug && git pull && hatch -e packaging run build-debug-binaries"
```

---

## 📝 Examples

### Example 1: Basic Deployment (Build Remote)
```bash
# Deploy to a standard Fedora server and build there
./scripts/packaging/deploy_fedora_debug.sh --build-remote myuser@fedora.example.com

# SSH and debug
ssh myuser@fedora.example.com
cd ~/snowflake-cli-debug
gdb ./dist/snow-debug/snow-debug
```

### Example 2: Custom Configuration
```bash
# Deploy with custom SSH port and key
./scripts/packaging/deploy_fedora_debug.sh \
    --build-remote \
    -p 2222 \
    -k ~/.ssh/my-key \
    -d /opt/debug \
    myuser@fedora.example.com
```

### Example 3: Manual Build After Setup
```bash
# Deploy setup files only
./scripts/packaging/deploy_fedora_debug.sh \
    --setup-only \
    myuser@fedora.example.com

# SSH and build manually
ssh myuser@fedora.example.com
cd ~/snowflake-cli-debug
./setup_fedora_debug.sh
git clone https://github.com/snowflakedb/snowflake-cli.git
cd snowflake-cli
hatch -e packaging run build-debug-binaries
gdb ./dist/snow-debug/snow-debug
```

That's it! You now have a fully functional debug build of the Snowflake CLI running on your remote Fedora system. 🎉
