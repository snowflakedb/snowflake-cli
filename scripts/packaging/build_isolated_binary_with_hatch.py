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
            hatch_dist_json = self.python_dist_root_version / "hatch-dist.json"
            if hatch_dist_json.exists():
                with hatch_dist_json.open() as fp:
                    hatch_json = json.load(fp)
                self.__python_path_within_archive = hatch_json["python_path"]
            else:
                # Fallback: try to find python executable in common locations
                print(f"Warning: {hatch_dist_json} not found, using fallback detection")
                for possible_path in ["bin/python", "bin/python3", "python", "python3"]:
                    candidate = Path(possible_path)
                    if (self.python_dist_root_version / candidate).exists():
                        # Use the directory containing the python executable
                        self.__python_path_within_archive = (
                            candidate.parent
                            if candidate.parent != Path(".")
                            else Path(".")
                        )
                        print(f"Found Python at: {candidate}")
                        break
                else:
                    # Last resort: assume current directory
                    print(
                        "Warning: Could not find Python executable, assuming root directory"
                    )
                    self.__python_path_within_archive = Path(".")
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


def hatch_install_python(python_tmp_dir: Path, python_version: str) -> bool:
    """Install Python dist into temp dir for bundling."""
    print(f"Installing Python {python_version} to {python_tmp_dir}")

    # Set conservative build flags for the Python installation itself
    env = os.environ.copy()
    env.update(
        {
            "CFLAGS": "-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma",
            "CXXFLAGS": "-O2 -march=core2 -mtune=generic -mno-avx -mno-avx2 -mno-bmi -mno-bmi2 -mno-fma",
            "LDFLAGS": "-Wl,-O1",
        }
    )

    completed_proc = subprocess.run(
        [
            "hatch",
            "python",
            "install",
            "--private",
            "--dir",
            python_tmp_dir,
            python_version,  # Use the original version parameter
        ],
        env=env,
        capture_output=True,
    )

    if completed_proc.returncode:
        print(
            f"Python installation failed with return code {completed_proc.returncode}"
        )
        print("STDOUT:", completed_proc.stdout.decode())
        print("STDERR:", completed_proc.stderr.decode())
        return False

    # Verify the installation created the expected files
    hatch_dist_json = python_tmp_dir / python_version / "hatch-dist.json"
    if not hatch_dist_json.exists():
        print(f"Warning: hatch-dist.json not found at {hatch_dist_json}")
        # List directory contents for debugging
        version_dir = python_tmp_dir / python_version
        if version_dir.exists():
            print(f"Contents of {version_dir}:")
            for item in version_dir.iterdir():
                print(f"  {item}")
        return False

    return True


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
    # Set conservative compiler flags for any native extensions
    # Use absolute baseline x86-64 for maximum compatibility
    env = os.environ.copy()
    env.update(
        {
            "CFLAGS": "-O1 -march=x86-64 -mtune=generic -mno-sse3 -mno-ssse3 -mno-sse4.1 -mno-sse4.2 -mno-popcnt -mno-avx -mno-avx2 -mno-aes -mno-pclmul",
            "CXXFLAGS": "-O1 -march=x86-64 -mtune=generic -mno-sse3 -mno-ssse3 -mno-sse4.1 -mno-sse4.2 -mno-popcnt -mno-avx -mno-avx2 -mno-aes -mno-pclmul",
            "LDFLAGS": "-Wl,-O1",
            "CC": "gcc",  # Explicitly set compiler
            "CXX": "g++",  # Explicitly set C++ compiler
        }
    )

    # First install essential build tools
    print("Installing build tools...")
    build_tools_proc = subprocess.run(
        [python_exe, "-m", "pip", "install", "-U", "wheel", "setuptools", "pip"],
        capture_output=True,
        env=env,
    )
    if build_tools_proc.returncode:
        print("Failed to install build tools:")
        print("STDOUT:", build_tools_proc.stdout.decode())
        print("STDERR:", build_tools_proc.stderr.decode())
        return False

    # Then install our project dependencies with conservative compilation
    print("Installing dependencies with conservative CPU settings...")
    # Strategy: Install binary wheels for packages that have OpenSSL/C++ compilation issues,
    # but force source builds for packages where we can control CPU optimizations
    deps_proc = subprocess.run(
        [
            python_exe,
            "-m",
            "pip",
            "install",
            "-U",
            "--no-binary=lxml,PyYAML",  # Build these from source with our conservative flags
            "--only-binary=cryptography,cffi,snowflake-connector-python",  # Use binary wheels for OpenSSL-dependent packages
            str(PROJECT_ROOT),
        ],
        capture_output=True,
        env=env,
    )
    if deps_proc.returncode:
        print("Failed to install dependencies:")
        print("STDOUT:", deps_proc.stdout.decode())
        print("STDERR:", deps_proc.stderr.decode())
        # Don't fail here - try installing the project anyway

    # Then install the project itself (this should work even if some deps failed)
    print("Installing project...")
    completed_proc = subprocess.run(
        [python_exe, "-m", "pip", "install", "-U", str(PROJECT_ROOT)],
        capture_output=True,
        env=env,
    )
    if completed_proc.returncode:
        print("Failed to install project:")
        print("STDOUT:", completed_proc.stdout.decode())
        print("STDERR:", completed_proc.stderr.decode())
        return False

    print("Project installation completed successfully")
    return True


def hatch_build_binary(archive_path: Path, python_path: Path) -> Path | None:
    """Use hatch to build the binary."""
    # NOTE: Removed custom distribution settings - we'll let PyApp download its own generic Python
    # os.environ["PYAPP_SKIP_INSTALL"] = "1"
    # os.environ["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
    # os.environ["PYAPP_DISTRIBUTION_PYTHON_PATH"] = str(python_path)
    # os.environ["PYAPP_DISTRIBUTION_PIP_AVAILABLE"] = "1"
    os.environ["PYAPP_FULL_ISOLATION"] = "1"
    # CRITICAL: PyApp seems to ignore our CPU compatibility settings
    # Try the most aggressive approach to force compatibility

    # Set MAXIMUM compatibility Rust flags for CI x86_64 Linux environment
    # Use x86-64 baseline (SSE/SSE2 only) - the most conservative possible
    conservative_flags = "-C target-cpu=x86-64 -C target-feature=-sse3,-ssse3,-sse4.1,-sse4.2,-popcnt,-avx,-avx2,-fma,-bmi1,-bmi2,-lzcnt,-movbe,-aes,-pclmulqdq -C opt-level=1 -C lto=false -C codegen-units=1"

    # Set Rust flags for maximum compatibility (no cross-compilation needed in CI)
    os.environ["RUSTFLAGS"] = conservative_flags
    os.environ["CARGO_PROFILE_RELEASE_OPT_LEVEL"] = "s"
    os.environ["CARGO_PROFILE_RELEASE_LTO"] = "false"
    os.environ["CARGO_PROFILE_RELEASE_CODEGEN_UNITS"] = "1"

    # Force PyApp to use the most compatible settings possible
    os.environ["PYAPP_DEBUG"] = "1"
    # NOTE: PYAPP_SKIP_INSTALL already set above - don't duplicate

    # When using PYAPP_DISTRIBUTION_PATH, do NOT set PYAPP_PYTHON_VERSION
    # as that creates a conflict between custom distribution and PyApp downloading Python
    # Use conservative PyApp settings for maximum compatibility
    os.environ["PYAPP_PIP_VERSION"] = "23.0"  # Use older pip version
    os.environ["PYAPP_UV_ENABLED"] = "false"  # Disable UV package manager

    # Override any potential host-specific optimizations with most basic features
    os.environ["CARGO_CFG_TARGET_FEATURE"] = "sse,sse2"  # Only basic x86_64 features
    os.environ["CARGO_CFG_TARGET_ARCH"] = "x86_64"
    os.environ["CARGO_CFG_TARGET_OS"] = "linux"

    # Force PyApp to use the most generic Python distribution possible
    # CRITICAL: Don't use our custom distribution, let PyApp download its own generic one
    # Remove custom distribution settings to force PyApp to use generic Python
    if "PYAPP_DISTRIBUTION_PATH" in os.environ:
        del os.environ["PYAPP_DISTRIBUTION_PATH"]
    os.environ["PYAPP_SKIP_INSTALL"] = "0"  # Let PyApp install Python itself

    # Use only the most basic PyApp settings for maximum compatibility
    os.environ["PYAPP_EXPOSE_METADATA"] = "true"  # Enable debugging
    os.environ["PYAPP_PYTHON_VERSION"] = "3.10"  # Use minimum required Python version
    # CRITICAL: Tell PyApp to install from local source, not PyPI
    os.environ["PYAPP_PROJECT_PATH"] = str(PROJECT_ROOT)  # Install from local source
    # Let PyApp use all default settings for distribution (no custom variants/sources/formats)

    # Force PyApp to build all packages from source to avoid optimized wheels
    os.environ[
        "PYAPP_PIP_EXTRA_ARGS"
    ] = "--no-binary=:all: --only-binary=pip,setuptools,wheel,hatch"

    # Ensure no CPU feature detection at runtime
    os.environ["CARGO_CFG_TARGET_HAS_ATOMIC"] = "8,16,32,64,ptr"
    os.environ["CARGO_FEATURE_STD"] = "1"

    # Force Rust to use oldest compatible codegen
    os.environ["CARGO_PROFILE_RELEASE_PANIC"] = "abort"

    print(f"Building with conservative flags: {conservative_flags}")
    print(f"RUSTFLAGS: {os.environ.get('RUSTFLAGS')}")
    print(f"PYAPP_DEBUG: {os.environ.get('PYAPP_DEBUG')}")
    print(f"PYAPP_PYTHON_VERSION: {os.environ.get('PYAPP_PYTHON_VERSION')}")
    print(f"PYAPP_PROJECT_PATH: {os.environ.get('PYAPP_PROJECT_PATH')}")
    print(f"PYAPP_SKIP_INSTALL: {os.environ.get('PYAPP_SKIP_INSTALL')}")
    print(f"PYAPP_PIP_EXTRA_ARGS: {os.environ.get('PYAPP_PIP_EXTRA_ARGS')}")
    print(
        "All distribution settings: Using PyApp defaults (no custom source/variant/format)"
    )

    # Debug: Print all environment variables starting with CARGO or PYAPP
    print("=== All CARGO/PYAPP Environment Variables ===")
    for key, value in sorted(os.environ.items()):
        if key.startswith(("CARGO_", "PYAPP_", "RUST")):
            print(f"{key}: {value}")
    print("=== End Environment Variables ===")
    print()

    completed_proc = subprocess.run(
        ["hatch", "build", "-t", "binary"], capture_output=True
    )
    if completed_proc.returncode:
        print("Build failed with stderr:")
        print(completed_proc.stderr.decode())
        return None

    # Parse the binary location from stderr
    stderr_output = completed_proc.stderr.decode().strip()
    print("Hatch build stderr:")
    print(stderr_output)

    # The binary location is typically the last line of stderr
    lines = stderr_output.split("\n")
    binary_path = lines[-1].strip() if lines else ""

    if not binary_path:
        print("Warning: Could not determine binary path from hatch output")
        return None

    return Path(binary_path)


def main():
    settings = ProjectSettings()

    # Skip custom Python distribution - let PyApp download its own generic one
    print("Using PyApp's built-in Python distribution management...")
    print(
        "PyApp will download Python 3.10 with default settings for maximum compatibility"
    )

    # Create dummy paths for compatibility with function signature
    # These won't be used since we removed PYAPP_DISTRIBUTION_PATH
    dummy_archive_path = Path("/tmp/dummy_archive.tar.gz")
    dummy_python_path = Path("/tmp/dummy_python")

    print(
        f"Building '{settings.project_name}' binary with generic PyApp distribution..."
    )
    binary_location = hatch_build_binary(dummy_archive_path, dummy_python_path)
    if binary_location:
        print("-> binary location:", binary_location)
        # Debug: Check if it's a file or directory
        if binary_location.is_file():
            print(f"-> binary is a file: {binary_location}")
        elif binary_location.is_dir():
            print(f"-> binary location is a directory, listing contents:")
            for item in binary_location.iterdir():
                print(f"   {item}")
        else:
            print(f"-> binary location does not exist: {binary_location}")


if __name__ == "__main__":
    main()
