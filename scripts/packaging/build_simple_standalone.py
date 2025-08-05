#!/usr/bin/env python3
"""
Alternative build script that creates a simple standalone Python application
without PyApp, for maximum CPU compatibility.
"""

import os
import re
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

    # Copy the actual Python executable instead of relying on symlinks
    print("Making Python executable portable...")
    try:
        # Find the real Python executable (not a symlink)
        real_python = Path(sys.executable).resolve()
        print(f"Source Python: {real_python}")

        # Remove all existing Python files/symlinks in venv
        for py_file in venv_bin.glob("python*"):
            if py_file.is_file() or py_file.is_symlink():
                print(f"Removing {py_file}")
                py_file.unlink()

        # Copy the actual Python binary to make it portable
        target_python = venv_bin / "python"
        print(f"Copying Python: {real_python} -> {target_python}")
        shutil.copy2(real_python, target_python)
        target_python.chmod(0o755)  # Make executable

        # Create standard symlinks to the copied executable
        python3_link = venv_bin / "python3"
        python3_link.symlink_to("python")  # Create relative symlink

        # Try to determine Python version and create version-specific symlink
        try:
            version_output = subprocess.run(
                [str(target_python), "--version"], capture_output=True, text=True
            )
            if version_output.returncode == 0:
                version_str = version_output.stdout.strip()
                # Extract version like "3.10" from "Python 3.10.12"
                version_match = re.search(r"Python (\d+\.\d+)", version_str)
                if version_match:
                    version = version_match.group(1)
                    version_link = venv_bin / f"python{version}"
                    version_link.symlink_to("python")
                    print(f"Created version-specific symlink: python{version}")
        except Exception as e:
            print(f"Could not create version-specific symlink: {e}")

        print("Successfully created portable Python executable and symlinks")

    except Exception as e:
        print(f"Failed to fix Python executable: {e}")
        print("Trying alternative approach...")

        # Alternative: try to find and use an existing non-symlink Python
        python_executable = None
        for candidate in ["python3.10", "python3.11", "python3.12", "python3.13"]:
            candidate_path = venv_bin / candidate
            if candidate_path.exists() and not candidate_path.is_symlink():
                python_executable = candidate
                break

        if python_executable:
            # Create symlinks to the found executable
            for symlink_name in ["python", "python3"]:
                symlink_path = venv_bin / symlink_name
                if symlink_path.exists():
                    symlink_path.unlink()
                symlink_path.symlink_to(python_executable)
            print(f"Using existing {python_executable} as Python executable")
        else:
            print("Warning: Could not fix Python executable, package may not work")
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
