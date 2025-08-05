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
