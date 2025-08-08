"""
Written based on https://github.com/hobbsd/hatch-build-isolated-binary

Use hatch to build a binary that doesn't require any network connection.

Installs a Python distribution to a dir in TMP; installs the project
wheel into that distribution; makes a bzipped archive of the distribution; then
builds the binary with the distribution embedded.

Run this script from the project root dir.
"""

import contextlib
import json
import os
import subprocess
import tarfile
import tempfile
from pathlib import Path

import tomlkit

PROJECT_ROOT = Path(__file__).parent.parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INSTALLATION_SOURCE_VARIABLE = "INSTALLATION_SOURCE"


@contextlib.contextmanager
def contextlib_chdir(path: Path):
    # re-implement contextlib.chdir to be available in python 3.10 (current build version)
    old_cwd = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(old_cwd)


class ProjectSettings:
    """Class for holding dynamically determined build settings."""

    def __init__(self) -> None:
        data: dict = self.get_pyproject_data()
        self.pyproject_data = data
        self.project_name: str = data["project"]["name"]
        self.python_version: str = data["tool"]["hatch"]["envs"]["packaging"]["python"]
        self.project_version: str = self.get_project_version()
        self._python_tmp_dir_object = tempfile.TemporaryDirectory(
            suffix=f"{self.project_name}-{self.project_version}"
        )
        self.python_tmp_dir = Path(self._python_tmp_dir_object.name)
        self.python_dist_root_version = Path(self.python_tmp_dir / self.python_version)
        # This is the path to the python executable within the distribution archive
        self.__python_path_within_archive: Path | None = None

    @staticmethod
    def get_project_version() -> str:
        """Use hatch to get the project version."""
        completed_proc = subprocess.run(["hatch", "version"], capture_output=True)
        return completed_proc.stdout.decode().strip()

    @staticmethod
    def get_pyproject_data() -> dict:
        """Retrieve the pyproject.toml data."""
        return tomlkit.parse(PYPROJECT_PATH.read_text())

    @property
    def python_path_within_archive(self) -> Path:
        """Returns the path to the root of the Python dist that we'll bundle."""
        if self.__python_path_within_archive is None:
            with (self.python_dist_root_version / "hatch-dist.json").open() as fp:
                hatch_json = json.load(fp)
            self.__python_path_within_archive = hatch_json["python_path"]
        return self.__python_path_within_archive

    @property
    def python_dist_exe(self) -> Path:
        """The full path to the distribution's python executable.

        Used for running 'pip install'.
        """
        return self.python_dist_root_version / self.python_path_within_archive


def make_project_wheel() -> Path:
    """Return path to the project's wheel build."""
    completed_proc = subprocess.run(
        ["hatch", "build", "-t", "wheel"], capture_output=True
    )
    return Path(completed_proc.stderr.decode().strip())


def make_dist_archive(python_tmp_dir: Path, dist_path: Path) -> Path:
    """Make and return path to tar-bzipped Python distribution."""
    archive = python_tmp_dir / "python.bz2"
    with contextlib_chdir(dist_path):
        with tarfile.open(archive, mode="w:bz2") as tar:
            tar.add(".")
    return archive


def copy_and_relocate_system_python(python_tmp_dir: Path, python_version: str) -> bool:
    """Copy our conservatively compiled system Python and make it relocatable."""
    import os
    import shutil
    import sys

    # Use the actual system Python we built, not the hatch virtual environment
    # Check if we're in a virtual environment and find the real system Python
    if hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    ):
        # We're in a virtual environment, use the actual system Python
        system_python = "/usr/local/bin/python"
        system_python_dir = Path("/usr/local")
        print("Detected virtual environment, using actual system Python")
    else:
        system_python = sys.executable
        system_python_dir = Path(sys.executable).parent.parent

    print(f"Using conservatively compiled system Python: {system_python}")
    print(f"System Python directory: {system_python_dir}")

    # Verify we're using the right Python before copying
    if not Path(system_python).exists():
        raise RuntimeError(f"System Python not found at {system_python}")

    # Check if this is our conservatively compiled Python
    import subprocess

    result = subprocess.run(
        [
            system_python,
            "-c",
            "import sysconfig; print('CFLAGS:', sysconfig.get_config_var('CFLAGS'))",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"Source Python verification: {result.stdout.strip()}")
        if "march=x86-64" in result.stdout and "mno-avx" in result.stdout:
            print("✅ Confirmed: Using conservatively compiled Python")
        else:
            print("⚠️  Warning: Python may not have conservative CPU flags")
    else:
        print(f"Warning: Could not verify source Python: {result.stderr}")

    # Create target directory structure
    target_python_dir = python_tmp_dir / python_version
    target_python_dir.mkdir(parents=True, exist_ok=True)

    # Copy the entire Python installation
    try:
        shutil.copytree(system_python_dir, target_python_dir, dirs_exist_ok=True)

        # Copy essential system libraries that Python needs
        lib_dir = target_python_dir / "lib"
        lib_dir.mkdir(exist_ok=True)

        # Copy essential system libraries that Python and packages need
        essential_libs = [
            # OpenSSL libraries for ssl module
            "/usr/lib/x86_64-linux-gnu/libssl.so.1.1",
            "/usr/lib/x86_64-linux-gnu/libcrypto.so.1.1",
            "/lib/x86_64-linux-gnu/libssl.so.1.1",
            "/lib/x86_64-linux-gnu/libcrypto.so.1.1",
            # Zlib for compression (needed by cryptography)
            "/usr/lib/x86_64-linux-gnu/libz.so.1",
            "/lib/x86_64-linux-gnu/libz.so.1",
            # FFI library
            "/usr/lib/x86_64-linux-gnu/libffi.so.6",
            "/lib/x86_64-linux-gnu/libffi.so.6",
            # Other commonly needed libraries
            "/usr/lib/x86_64-linux-gnu/libbz2.so.1.0",
            "/lib/x86_64-linux-gnu/libbz2.so.1.0",
            "/usr/lib/x86_64-linux-gnu/liblzma.so.5",
            "/lib/x86_64-linux-gnu/liblzma.so.5",
        ]

        copied_libs = []
        for lib_path in essential_libs:
            if Path(lib_path).exists():
                lib_name = Path(lib_path).name
                shutil.copy2(lib_path, lib_dir / lib_name)
                copied_libs.append(lib_name)
                print(f"Copied essential library: {lib_name}")

        print(f"Total essential libraries copied: {len(copied_libs)}")
        if not copied_libs:
            print("⚠️  Warning: No essential libraries were found to copy")

        # Create lib64 symlink if needed (some systems expect this)
        lib64_dir = target_python_dir / "lib64"
        if not lib64_dir.exists():
            lib64_dir.symlink_to("lib")
            print("Created lib64 -> lib symlink")

        # Create a wrapper script that sets PYTHONHOME correctly
        python_exe = target_python_dir / "bin" / "python"
        python_wrapper = target_python_dir / "bin" / "python_wrapper"

        # Create wrapper script
        wrapper_content = f"""#!/bin/bash
# Auto-generated wrapper for relocatable Python
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
PYTHON_HOME="$(dirname "$SCRIPT_DIR")"
export PYTHONHOME="$PYTHON_HOME"
export PYTHONPATH="$PYTHON_HOME/lib/python3.10:$PYTHON_HOME/lib/python3.10/lib-dynload:$PYTHON_HOME/lib/python3.10/site-packages"
export LD_LIBRARY_PATH="$PYTHON_HOME/lib:$PYTHON_HOME/lib64:$LD_LIBRARY_PATH"
exec "$SCRIPT_DIR/python" "$@"
"""

        with open(python_wrapper, "w") as f:
            f.write(wrapper_content)

        os.chmod(python_wrapper, 0o755)
        print(f"Created Python wrapper: {python_wrapper}")

        # Test the wrapper
        import subprocess

        test_result = subprocess.run(
            [
                str(python_wrapper),
                "-c",
                "import sys; print('Wrapper test successful - Python:', sys.version[:20])",
            ],
            capture_output=True,
            text=True,
        )
        if test_result.returncode == 0:
            print(f"Python wrapper test: {test_result.stdout.strip()}")
        else:
            print(f"Python wrapper test failed: {test_result.stderr}")

        # Create hatch-dist.json to use our wrapper
        import json

        hatch_dist_info = {"python_path": "bin/python_wrapper"}

        with open(target_python_dir / "hatch-dist.json", "w") as f:
            json.dump(hatch_dist_info, f)

        print(
            f"Successfully copied and configured conservative Python to {target_python_dir}"
        )
        return True
    except Exception as e:
        print(f"Error copying system Python: {e}")
        import traceback

        traceback.print_exc()
        return False


@contextlib.contextmanager
def override_is_installation_source_variable():
    about_file = PROJECT_ROOT / "src" / "snowflake" / "cli" / "__about__.py"
    contents = about_file.read_text()
    if INSTALLATION_SOURCE_VARIABLE not in contents:
        raise RuntimeError(
            f"{INSTALLATION_SOURCE_VARIABLE} variable not defined in __about__.py"
        )
    about_file.write_text(
        contents.replace(
            f"{INSTALLATION_SOURCE_VARIABLE} = CLIInstallationSource.PYPI",
            f"{INSTALLATION_SOURCE_VARIABLE} = CLIInstallationSource.BINARY",
        )
    )
    yield
    subprocess.run(["git", "checkout", str(about_file)])


def pip_install_project(python_exe: str) -> bool:
    """Install the project into the Python distribution."""
    completed_proc = subprocess.run(
        [
            python_exe,
            "-m",
            "pip",
            "install",
            "--only-binary=cryptography,cffi,pycparser,setuptools-rust",
            "--no-binary=snowflake-cli",
            "-U",
            str(PROJECT_ROOT),
        ],
        capture_output=True,
    )
    return not completed_proc.returncode


def hatch_build_binary(archive_path: Path, python_path: Path) -> Path | None:
    """Use hatch to build the binary."""
    os.environ["PYAPP_SKIP_INSTALL"] = "1"
    os.environ["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
    os.environ["PYAPP_FULL_ISOLATION"] = "1"
    os.environ["PYAPP_DISTRIBUTION_PYTHON_PATH"] = str(python_path)
    os.environ["PYAPP_DISTRIBUTION_PIP_AVAILABLE"] = "1"
    completed_proc = subprocess.run(
        ["hatch", "build", "-t", "binary"], capture_output=True
    )
    if completed_proc.returncode:
        print(completed_proc.stderr)
        return None
    # The binary location is the last line of stderr
    return Path(completed_proc.stderr.decode().split()[-1])


def main():
    settings = ProjectSettings()
    print("Copying and configuring conservatively compiled system Python...")
    copy_and_relocate_system_python(settings.python_tmp_dir, settings.python_version)
    print("-> configured")

    print(f"Installing project into Python distribution...")
    print(f"Target Python executable: {settings.python_dist_exe}")

    # Verify we're using our conservative Python
    import subprocess

    result = subprocess.run(
        [
            str(settings.python_dist_exe),
            "-c",
            "import sysconfig; print('Using Python with CFLAGS:', sysconfig.get_config_var('CFLAGS'))",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"Python verification: {result.stdout.strip()}")
    else:
        print(f"Warning: Could not verify Python flags: {result.stderr}")

    with override_is_installation_source_variable():
        pip_install_project(str(settings.python_dist_exe))
    print("-> installed")

    print("Making distribution archive...")
    archive_path = make_dist_archive(
        settings.python_tmp_dir, settings.python_dist_root_version
    )
    print("->", archive_path)

    print(f"Building '{settings.project_name}' binary...")
    binary_location = hatch_build_binary(
        archive_path, settings.python_path_within_archive
    )
    if binary_location:
        print("-> binary location:", binary_location)


if __name__ == "__main__":
    main()
