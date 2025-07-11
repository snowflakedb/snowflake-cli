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

# Debug Build with GDB Information

This document explains how to build and use the debug version of the Snowflake CLI with GDB debugging information.

## Overview

The debug build includes debugging symbols and is configured to work with GDB (GNU Debugger) for troubleshooting and development purposes. The debug binary is larger and slower than the release version but provides detailed debugging information.

## Prerequisites

### System Requirements

- **Linux or macOS**: The debug build currently supports Linux and macOS
- **GDB**: Install GDB on your system
  - **Fedora/RHEL/CentOS**: `sudo dnf install gdb` or `sudo yum install gdb`
  - **Ubuntu/Debian**: `sudo apt-get install gdb`
  - **macOS**: `brew install gdb` (note: may require additional codesigning setup)
- **Rust**: Required for building PyApp
- **Python 3.10+**: Required for the build process

### Fedora-Specific Setup

For Fedora systems, install the required packages:

```bash
# Install build dependencies
sudo dnf install -y gdb python3-devel gcc gcc-c++ make

# Install Python debug symbols (optional but recommended)
sudo dnf install -y python3-debug python3-debuginfo

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
```

### Optional: Python Debug Symbols

For better debugging experience, install Python debug symbols:

- **Fedora/RHEL/CentOS**: `sudo dnf install python3-debuginfo` or `sudo yum install python3-debuginfo`
- **Ubuntu/Debian**: `sudo apt-get install python3-dbg`
- **macOS**: Debug symbols are typically included with Python installations

## Building the Debug Binary

### Method 1: Using Hatch (Recommended)

```bash
# Build the debug binary using hatch
hatch -e packaging run build-debug-binaries
```

### Method 2: Using the Script Directly

```bash
# Build using the shell script directly
./scripts/packaging/build_debug_binaries.sh
```

### Method 3: Using Python Script Only

```bash
# Build using just the Python script
hatch -e packaging run build-debug-binary
```

## Build Output

The debug binary will be created in:
```
dist/snow-debug/snow-debug
```

## Remote Deployment (Fedora)

### Transferring the Debug Binary

After building locally, transfer to your remote Fedora system:

```bash
# Using scp
scp dist/snow-debug/snow-debug user@fedora-host:/path/to/destination/

# Or using rsync
rsync -av dist/snow-debug/ user@fedora-host:/path/to/destination/
```

### Building Directly on Fedora

If you prefer to build directly on the Fedora system:

```bash
# Clone the repository
git clone https://github.com/snowflakedb/snowflake-cli.git
cd snowflake-cli

# Install dependencies
sudo dnf install -y python3-pip python3-devel gcc gcc-c++ make git

# Install hatch
pip3 install --user hatch

# Build the debug binary
hatch -e packaging run build-debug-binaries
```

## Using the Debug Binary

### Basic Usage

The debug binary works exactly like the regular CLI:

```bash
# Run the debug binary
./dist/snow-debug/snow-debug --help
./dist/snow-debug/snow-debug --version
```

### Debugging with GDB on Fedora

#### Starting GDB

```bash
# Launch the debug binary with GDB
gdb ./dist/snow-debug/snow-debug
```

#### Fedora-Specific GDB Configuration

On Fedora, you may need to configure GDB for better Python debugging:

```bash
# Check if Python debugging extension is available
ls /usr/share/gdb/auto-load/usr/bin/python*

# If available, it should auto-load. If not, you can load it manually:
# (gdb) source /usr/share/gdb/auto-load/usr/bin/python3.*-gdb.py
```

#### Basic GDB Commands

Once in GDB:

```bash
# Set breakpoints (if debugging source code)
(gdb) break main
(gdb) break src/snowflake/cli/_app/__main__.py:main

# Run the program with arguments
(gdb) run --help
(gdb) run connection list

# When execution stops, examine the stack
(gdb) backtrace
(gdb) bt

# Step through execution
(gdb) next
(gdb) step
(gdb) continue

# Examine variables
(gdb) info locals
(gdb) print variable_name

# For Python debugging (if Python debugging extension is loaded)
(gdb) py-bt
(gdb) py-list
(gdb) py-locals
```

#### Debugging Python Code

If you have Python debugging symbols installed, you can use Python-specific GDB commands:

```bash
# Load Python debugging extension (if not auto-loaded)
(gdb) source /usr/share/gdb/auto-load/usr/bin/python3.*-gdb.py

# Python-specific commands
(gdb) py-bt        # Python backtrace
(gdb) py-list      # Show Python source code
(gdb) py-locals    # Show Python local variables
(gdb) py-up        # Move up Python stack frame
(gdb) py-down      # Move down Python stack frame
```

#### Remote Debugging

For remote debugging on Fedora, you can use gdbserver:

```bash
# On the remote Fedora system, start gdbserver
gdbserver :2345 ./dist/snow-debug/snow-debug --help

# On your local machine, connect to the remote gdbserver
gdb ./dist/snow-debug/snow-debug
(gdb) target remote fedora-host:2345
```

#### Debugging Core Dumps

If the application crashes, you can analyze core dumps:

```bash
# Enable core dumps on Fedora
ulimit -c unlimited

# Configure systemd to generate core dumps (if using systemd)
sudo sysctl kernel.core_pattern=/tmp/core.%e.%p.%h.%t

# Run the application (it will create a core file if it crashes)
./dist/snow-debug/snow-debug some-command

# Debug the core dump
gdb ./dist/snow-debug/snow-debug /tmp/core.*
```

## Environment Variables

The debug build supports additional environment variables for debugging:

```bash
# Enable Rust backtraces
export RUST_BACKTRACE=full

# Enable Python debugging
export PYTHONDEVMODE=1

# Enable verbose logging
export SNOWFLAKE_CLI_DEBUG=1

# For debugging on Fedora with SELinux
export PYTHONDONTWRITEBYTECODE=1
```

## Fedora-Specific Troubleshooting

### SELinux Issues

If you encounter SELinux-related issues:

```bash
# Check SELinux status
getenforce

# If needed, temporarily disable SELinux for debugging
sudo setenforce 0

# Or create a custom SELinux policy for your debugging session
# (consult SELinux documentation for proper policy creation)
```

### GDB Not Finding Symbols

If GDB shows "no debugging symbols found" on Fedora:

1. Ensure you built with the debug script
2. Check that the binary has debug symbols: `file ./dist/snow-debug/snow-debug`
3. You should see "with debug_info, not stripped" in the output
4. Install debuginfo packages: `sudo dnf install python3-debuginfo`

### Python Debugging Not Working

If Python debugging commands (py-bt, py-list) don't work on Fedora:

1. Install Python debug symbols: `sudo dnf install python3-debuginfo`
2. Ensure the Python debugging extension is loaded
3. Try loading it manually: `source /usr/share/gdb/auto-load/usr/bin/python*-gdb.py`

### Performance Issues

The debug binary is intentionally slower than the release version due to:
- Disabled optimizations (-O0)
- Additional debugging information
- Debug assertions and checks

This is normal and expected for debug builds.

## Fedora Package Information

### Checking Debug Packages

```bash
# List available debug packages
dnf list available "*debug*" | grep python

# Check if debuginfo is installed
rpm -qa | grep debuginfo

# Get information about debug packages
dnf info python3-debuginfo
```

### Installing Additional Debug Tools

```bash
# Install additional debugging tools
sudo dnf install -y valgrind strace ltrace

# Install development tools
sudo dnf groupinstall -y "Development Tools"
```

## Build Configuration Details

The debug build includes:

- **C/C++ Debug Flags**: `-g -O0` (full debug info, no optimization)
- **Rust Debug Flags**: `-C debuginfo=2` (full debug info)
- **PyApp Debug Mode**: Enabled with `PYAPP_DEBUG=1`
- **Python Debug Mode**: Compiled with debugging support where possible

## Contributing

If you encounter issues with the debug build or want to improve the debugging experience:

1. File an issue describing the problem
2. Include GDB output and system information
3. Specify what debugging scenario you're trying to accomplish

## Related Files

- `scripts/packaging/build_debug_binary.py` - Python script for building debug binaries
- `scripts/packaging/build_debug_binaries.sh` - Shell script wrapper
- `pyproject.toml` - Build configuration including debug targets
