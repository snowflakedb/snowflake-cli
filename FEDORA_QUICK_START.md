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

### Option 2: Deploy Pre-built Binary (Only if architectures match)

```bash
# Only use this if your local machine and remote system have the same architecture
./scripts/packaging/deploy_fedora_debug.sh user@your-fedora-server.com

# SSH to your server and start debugging
ssh user@your-fedora-server.com
cd ~/snowflake-cli-debug
gdb ./snow-debug
```

### Option 3: Manual Setup

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
