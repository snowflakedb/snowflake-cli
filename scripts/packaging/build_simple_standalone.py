#!/usr/bin/env python3
"""
Alternative build script that creates a simple standalone Python application
without PyApp, for maximum CPU compatibility.
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
DIST_DIR = PROJECT_ROOT / "dist"


def create_simple_standalone():
    """Create a simple standalone Python application without PyApp."""
    print("Creating simple standalone Python application...")

    # Clean up any previous builds
    standalone_dir = DIST_DIR / "standalone"
    if standalone_dir.exists():
        shutil.rmtree(standalone_dir)
    standalone_dir.mkdir(parents=True, exist_ok=True)

    # Create a virtual environment with conservative settings
    venv_dir = standalone_dir / "venv"
    print(f"Creating virtual environment in {venv_dir}")

    # Set conservative compiler flags for any native extensions
    env = os.environ.copy()
    env.update(
        {
            "CFLAGS": "-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma",
            "CXXFLAGS": "-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma",
            "LDFLAGS": "-Wl,-O1",
        }
    )

    # Create virtual environment
    subprocess.run([sys.executable, "-m", "venv", str(venv_dir)], env=env, check=True)

    # Get paths
    if sys.platform == "win32":
        python_exe = venv_dir / "Scripts" / "python.exe"
        pip_exe = venv_dir / "Scripts" / "pip.exe"
    else:
        python_exe = venv_dir / "bin" / "python"
        pip_exe = venv_dir / "bin" / "pip"

    # Upgrade pip and install build tools
    print("Installing build tools...")
    subprocess.run(
        [str(pip_exe), "install", "-U", "pip", "wheel", "setuptools"],
        env=env,
        check=True,
    )

    # Install project with conservative compilation for specific packages only
    print("Installing project...")

    # First install most dependencies normally (using pre-built wheels)
    subprocess.run(
        [str(pip_exe), "install", "-U", str(PROJECT_ROOT)], env=env, check=True
    )

    # Then try to rebuild only the most problematic packages from source
    problematic_packages = [
        "PyYAML"
    ]  # Start with just PyYAML as it's most likely to have CPU issues

    for package in problematic_packages:
        try:
            print(f"Rebuilding {package} from source with conservative settings...")
            subprocess.run(
                [
                    str(pip_exe),
                    "install",
                    "-U",
                    "--force-reinstall",
                    f"--no-binary={package}",
                    package,
                ],
                env=env,
                check=True,
            )
        except subprocess.CalledProcessError:
            print(
                f"Warning: Failed to rebuild {package} from source, using pre-built wheel"
            )

    # Create a simple launcher script
    launcher_script = standalone_dir / "snow"
    with open(launcher_script, "w") as f:
        f.write(
            f"""#!/bin/bash
# Simple launcher for snowflake-cli with maximum compatibility

# Determine script directory
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"

# Try different Python executable locations
# 1. For DEB package installation (absolute path)
PYTHON_EXE="/usr/lib/snowflake/snowflake-cli/venv/bin/python"
if [[ ! -x "$PYTHON_EXE" ]]; then
    # 2. For local testing (relative path)
    PYTHON_EXE="$SCRIPT_DIR/venv/bin/python"
fi

# Verify Python executable exists
if [[ ! -x "$PYTHON_EXE" ]]; then
    echo "Error: Cannot find Python executable for snowflake-cli"
    echo "Looked for:"
    echo "  /usr/lib/snowflake/snowflake-cli/venv/bin/python"
    echo "  $SCRIPT_DIR/venv/bin/python"
    exit 1
fi

# Export conservative settings in case any native modules need them
export CFLAGS="-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma"
export CXXFLAGS="$CFLAGS"

# Run snow CLI
exec "$PYTHON_EXE" -m snowflake.cli._app.__main__ "$@"
"""
        )

    # Make launcher executable
    launcher_script.chmod(0o755)

    # Fix Python symlinks in the virtual environment
    print("Fixing Python symlinks...")
    venv_bin = venv_dir / "bin"

    # Create a wrapper script that uses system Python instead of copying executable
    print("Creating system Python wrapper...")
    try:
        # Remove all existing Python files/symlinks in venv
        for py_file in venv_bin.glob("python*"):
            if py_file.is_file() or py_file.is_symlink():
                print(f"Removing {py_file}")
                py_file.unlink()

        # Create a wrapper script that uses system Python
        target_python = venv_bin / "python"
        with open(target_python, "w") as f:
            f.write(
                """#!/bin/bash
# Wrapper script that uses system Python for maximum compatibility

# Find the best available Python version
PYTHON=""

# Try Python versions in order of preference
for candidate in python3.12 python3.11 python3.10 python3.9 python3; do
    if command -v "$candidate" >/dev/null 2>&1; then
        PYTHON="$candidate"
        break
    fi
done

# If no Python found, try common installation paths
if [ -z "$PYTHON" ]; then
    for candidate in /usr/bin/python3 /usr/local/bin/python3 /opt/python*/bin/python3; do
        if [ -x "$candidate" ]; then
            PYTHON="$candidate"
            break
        fi
    done
fi

if [ -z "$PYTHON" ]; then
    echo "Error: No compatible Python 3.x found on system" >&2
    echo "Please install Python 3.9 or later. On Ubuntu/Debian:" >&2
    echo "  sudo apt update && sudo apt install python3" >&2
    echo "On RHEL/CentOS:" >&2
    echo "  sudo yum install python3" >&2
    exit 1
fi

# Execute Python with the same arguments, setting PYTHONPATH to our venv
# Dynamically determine the correct site-packages path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_ROOT="$(dirname "$SCRIPT_DIR")"

# Find the actual site-packages directory (there should be only one python3.x directory)
SITE_PACKAGES_DIR=$(find "$VENV_ROOT/lib" -name "python3.*" -type d 2>/dev/null | head -1)
if [ -n "$SITE_PACKAGES_DIR" ]; then
    SITE_PACKAGES="$SITE_PACKAGES_DIR/site-packages"
else
    # Fallback to common paths
    for version in 3.12 3.11 3.10 3.9; do
        CANDIDATE="$VENV_ROOT/lib/python$version/site-packages"
        if [ -d "$CANDIDATE" ]; then
            SITE_PACKAGES="$CANDIDATE"
            break
        fi
    done
fi

export PYTHONPATH="$SITE_PACKAGES:$PYTHONPATH"
exec "$PYTHON" "$@"
"""
            )
        target_python.chmod(0o755)  # Make executable

        # Create symlinks to the wrapper
        python3_link = venv_bin / "python3"
        python3_link.symlink_to("python")  # Create relative symlink

        # Create a version-agnostic link
        python310_link = venv_bin / "python3.10"
        python310_link.symlink_to("python")  # Create relative symlink

        print("Successfully created system Python wrapper and symlinks")

    except Exception as e:
        print(f"Failed to create Python wrapper: {e}")
        return False

    # Test the standalone installation
    print("Testing standalone installation...")
    result = subprocess.run(
        [str(launcher_script), "--version"], capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"SUCCESS: Standalone version works: {result.stdout.strip()}")
    else:
        print(f"ERROR: Standalone version failed: {result.stderr}")
        return False

    print(f"Standalone application created in: {standalone_dir}")
    print(f"Size: {get_dir_size(standalone_dir):.1f} MB")
    return True


def get_dir_size(path):
    """Get total size of directory in MB."""
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total += os.path.getsize(filepath)
    return total / (1024 * 1024)


if __name__ == "__main__":
    create_simple_standalone()
