"""
Written based on https://github.com/hobbsd/hatch-build-isolated-binary

Use hatch to build a binary that doesn't require any network connection.

Installs a Python distribution to a dir in TMP; installs the project
wheel into that distribution; makes a bzipped archive of the distribution; then
builds the binary with the distribution embedded.

Run this script from the project root dir.
"""

import argparse
import contextlib
import hashlib
import json
import os
import platform
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

import tomlkit

PROJECT_ROOT = Path(__file__).parent.parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INSTALLATION_SOURCE_VARIABLE = "INSTALLATION_SOURCE"
# BINARY is the existing pkg / MSI / deb / rpm stamp. SNOWFLAKE_MANAGED is the
# curl|sh tarball channel. Default stays BINARY so those jobs are unchanged.
INSTALLATION_SOURCE_STAMPS = ("BINARY", "SNOWFLAKE_MANAGED")
INSTALLATION_SOURCE_ENV = "SNOWFLAKE_CLI_INSTALLATION_SOURCE"
PACK_MANAGED_TARBALL_ENV = "SNOWFLAKE_CLI_PACK_MANAGED_TARBALL"
# uname / platform.machine() → tarball os/arch (Cortex mapping).
MANAGED_OS_NAMES = {
    "darwin": "darwin",
    "linux": "linux",
    "windows": "windows",
}
MANAGED_ARCH_NAMES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}
MANAGED_TARBALL_OS = frozenset({"darwin", "linux", "windows"})
MANAGED_TARBALL_ARCH = frozenset({"amd64", "arm64"})


def rewrite_installation_source_assignment(
    contents: str, source: str = "BINARY"
) -> str:
    if INSTALLATION_SOURCE_VARIABLE not in contents:
        raise RuntimeError(
            f"{INSTALLATION_SOURCE_VARIABLE} variable not defined in __about__.py"
        )
    if source not in INSTALLATION_SOURCE_STAMPS:
        raise ValueError(
            f"installation source stamp must be one of {INSTALLATION_SOURCE_STAMPS}, got {source!r}"
        )
    return contents.replace(
        f"{INSTALLATION_SOURCE_VARIABLE} = CLIInstallationSource.PYPI",
        f"{INSTALLATION_SOURCE_VARIABLE} = CLIInstallationSource.{source}",
    )


def resolve_installation_source_stamp(env: dict | None = None) -> str:
    values = os.environ if env is None else env
    source = values.get(INSTALLATION_SOURCE_ENV, "BINARY")
    if source not in INSTALLATION_SOURCE_STAMPS:
        raise ValueError(
            f"installation source stamp must be one of {INSTALLATION_SOURCE_STAMPS}, got {source!r}"
        )
    return source


def should_pack_managed_tarball(source: str, env: dict | None = None) -> bool:
    if source != "SNOWFLAKE_MANAGED":
        return False
    values = os.environ if env is None else env
    return values.get(PACK_MANAGED_TARBALL_ENV, "1") != "0"


def managed_platform(
    system: str | None = None, machine: str | None = None
) -> tuple[str, str]:
    os_key = (system or platform.system()).lower()
    arch_key = (machine or platform.machine()).lower()
    if os_key not in MANAGED_OS_NAMES:
        raise ValueError(f"unsupported snowflake-managed os {os_key!r}")
    if arch_key not in MANAGED_ARCH_NAMES:
        raise ValueError(f"unsupported snowflake-managed arch {arch_key!r}")
    return MANAGED_OS_NAMES[os_key], MANAGED_ARCH_NAMES[arch_key]


def managed_tarball_name(version: str, os_name: str, arch: str) -> str:
    return f"snowflake-cli-{version}-{os_name}-{arch}.tar.gz"


def managed_manifest_fragment_name(os_name: str, arch: str) -> str:
    return f"manifest-{os_name}-{arch}.json"


def managed_binary_arcname(os_name: str) -> str:
    return "snow.exe" if os_name == "windows" else "snow"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def pack_managed_tarball(
    binary_path: Path,
    dest_dir: Path,
    version: str,
    os_name: str,
    arch: str,
) -> tuple[Path, Path, dict]:
    """Write snowflake-cli-<ver>-<os>-<arch>.tar.gz plus a Releng merge fragment.

    The archive has the binary at the root as ``snow`` (or ``snow.exe`` on
    Windows) so install.sh / snow upgrade can unwrap it. Checksum is SHA-256
    of the tarball, Cortex-parity, fail closed on the client.
    """
    if os_name not in MANAGED_TARBALL_OS:
        raise ValueError(f"unsupported snowflake-managed os {os_name!r}")
    if arch not in MANAGED_TARBALL_ARCH:
        raise ValueError(f"unsupported snowflake-managed arch {arch!r}")
    if not binary_path.is_file():
        raise FileNotFoundError(f"managed binary not found: {binary_path}")

    dest_dir.mkdir(parents=True, exist_ok=True)
    tarball_name = managed_tarball_name(version, os_name, arch)
    tarball = dest_dir / tarball_name
    with tarfile.open(tarball, "w:gz") as tar:
        tar.add(binary_path, arcname=managed_binary_arcname(os_name))
    checksum = sha256_file(tarball)
    fragment = {
        "packages": {
            os_name: {
                arch: {
                    "name": tarball_name,
                    "checksum": checksum,
                }
            }
        }
    }
    fragment_path = dest_dir / managed_manifest_fragment_name(os_name, arch)
    fragment_path.write_text(json.dumps(fragment, indent=2) + "\n")
    return tarball, fragment_path, fragment


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


def hatch_install_python(python_tmp_dir: Path, python_version: str) -> bool:
    """Install Python distribution - platform specific approach."""
    import platform

    system = platform.system().lower()

    if system in ["darwin", "windows"]:
        platform_name = "macOS" if system == "darwin" else "Windows"
        print(f"Detected {platform_name}: Using original hatch python install approach")
        return install_python_original(python_tmp_dir, python_version)
    else:
        print("Detected Linux: Using optimized system Python approach")
        return install_python_linux(python_tmp_dir, python_version)


def install_python_original(python_tmp_dir: Path, python_version: str) -> bool:
    """Install Python dist using original hatch approach (macOS and Windows)."""
    # This is the original working approach from commit 8da461e3
    # Works for both macOS and Windows - provides complete Python installation
    completed_proc = subprocess.run(
        [
            "hatch",
            "python",
            "install",
            "--private",
            "--dir",
            python_tmp_dir,
            python_version,
        ]
    )
    return not completed_proc.returncode


def install_python_linux(python_tmp_dir: Path, python_version: str) -> bool:
    """Copy our conservatively compiled system Python for bundling (Linux)."""
    import shutil

    # Use the system Python we built instead of hatch installing one
    system_python_dir = Path("/usr/local")
    target_python_dir = python_tmp_dir / python_version

    print(f"Copying system Python from {system_python_dir} to {target_python_dir}")

    # Copy the entire system Python installation (ignore missing directories)
    shutil.copytree(
        system_python_dir,
        target_python_dir,
        dirs_exist_ok=True,
        ignore_dangling_symlinks=True,
    )

    # Copy essential system libraries that Python needs at runtime (architecture-aware)
    lib_dir = target_python_dir / "lib"
    lib_dir.mkdir(exist_ok=True)

    # Detect architecture for library paths
    import platform

    arch = platform.machine()
    if arch == "x86_64":
        arch_dir = "x86_64-linux-gnu"
    elif arch in ["aarch64", "arm64"]:
        arch_dir = "aarch64-linux-gnu"
    else:
        arch_dir = f"{arch}-linux-gnu"  # fallback

    print(f"Detected architecture: {arch}, using lib path: {arch_dir}")

    essential_lib_names = [
        "libssl.so.1.1",  # OpenSSL for ssl module
        "libcrypto.so.1.1",  # Crypto for ssl module
        "libz.so.1",  # Zlib for compression
        "libffi.so.6",  # FFI for ctypes module
    ]

    copied_libs = []
    for lib_name in essential_lib_names:
        # Try multiple possible locations
        possible_paths = [
            f"/usr/lib/{arch_dir}/{lib_name}",
            f"/lib/{arch_dir}/{lib_name}",
            f"/usr/lib/{lib_name}",
            f"/lib/{lib_name}",
        ]

        for lib_path in possible_paths:
            if Path(lib_path).exists():
                try:
                    shutil.copy2(lib_path, lib_dir / lib_name)
                    copied_libs.append(lib_name)
                    print(f"Copied essential library: {lib_name} from {lib_path}")
                    break
                except (OSError, IOError, PermissionError) as e:
                    print(f"Warning: Failed to copy {lib_name} from {lib_path}: {e}")
        else:
            print(f"Warning: Could not find {lib_name} in any standard location")

    print(f"Total essential libraries copied: {len(copied_libs)}")

    # Create a wrapper script that sets library paths
    python_wrapper = target_python_dir / "bin" / "python_wrapper"
    wrapper_content = f"""#!/bin/bash
# Auto-generated wrapper for relocatable Python
SCRIPT_DIR="$(cd "$(dirname "${{BASH_SOURCE[0]}}")" && pwd)"
PYTHON_HOME="$(dirname "$SCRIPT_DIR")"
export PYTHONHOME="$PYTHON_HOME"
export LD_LIBRARY_PATH="$PYTHON_HOME/lib:$LD_LIBRARY_PATH"
exec "$SCRIPT_DIR/python" "$@"
"""

    with open(python_wrapper, "w") as f:
        f.write(wrapper_content)

    import os

    os.chmod(python_wrapper, 0o755)
    print(f"Created Python wrapper: {python_wrapper}")

    # Create hatch-dist.json to point to our wrapper
    import json

    hatch_dist_info = {"python_path": "bin/python_wrapper"}
    with open(target_python_dir / "hatch-dist.json", "w") as f:
        json.dump(hatch_dist_info, f)

    print(f"Successfully copied system Python to {target_python_dir}")
    return True


@contextlib.contextmanager
def override_is_installation_source_variable(source: str = "BINARY"):
    about_file = PROJECT_ROOT / "src" / "snowflake" / "cli" / "__about__.py"
    contents = about_file.read_text()
    about_file.write_text(rewrite_installation_source_assignment(contents, source))
    yield
    subprocess.run(["git", "checkout", str(about_file)])


def pip_install_project(python_exe: str) -> bool:
    """Install the project into the Python distribution."""
    completed_proc = subprocess.run(
        [python_exe, "-m", "pip", "install", "-U", str(PROJECT_ROOT)],
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


def _pack_existing_binary(args: argparse.Namespace) -> None:
    binary_path = args.pack_tarball
    version = args.version or ProjectSettings.get_project_version()
    if args.os_name and args.arch:
        os_name, arch = args.os_name, args.arch
    else:
        detected_os, detected_arch = managed_platform()
        os_name = args.os_name or detected_os
        arch = args.arch or detected_arch
    dest_dir = args.dest_dir or (PROJECT_ROOT / "dist")
    tarball, fragment_path, _fragment = pack_managed_tarball(
        binary_path, dest_dir, version, os_name, arch
    )
    print("-> managed tarball:", tarball)
    print("-> manifest fragment:", fragment_path)


def build_isolated_binary() -> Path | None:
    source = resolve_installation_source_stamp()
    settings = ProjectSettings()
    print("Installing Python distribution to TMP dir...")
    hatch_install_python(settings.python_tmp_dir, settings.python_version)
    print("-> installed")

    print(f"Installing project into Python distribution...")
    print(f"-> installation source stamp: {source}")
    with override_is_installation_source_variable(source):
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
        if should_pack_managed_tarball(source):
            os_name, arch = managed_platform()
            dest = PROJECT_ROOT / "dist"
            tarball, fragment_path, _fragment = pack_managed_tarball(
                binary_location, dest, settings.project_version, os_name, arch
            )
            print("-> managed tarball:", tarball)
            print("-> manifest fragment:", fragment_path)
    return binary_location


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--pack-tarball",
        type=Path,
        help="Pack an already-built (and, on macOS, already-signed) binary.",
    )
    parser.add_argument("--version", help="CLI version for the tarball filename.")
    parser.add_argument(
        "--os-name",
        choices=sorted(MANAGED_TARBALL_OS),
        help="Tarball os (darwin|linux|windows). Default: this host.",
    )
    parser.add_argument(
        "--arch",
        choices=sorted(MANAGED_TARBALL_ARCH),
        help="Tarball arch (amd64|arm64). Default: this host.",
    )
    parser.add_argument(
        "--dest-dir",
        type=Path,
        help="Directory for the tarball and manifest fragment (default: dist/).",
    )
    args = parser.parse_args(argv)
    if args.pack_tarball is not None:
        _pack_existing_binary(args)
        return
    binary_location = build_isolated_binary()
    if binary_location is None:
        print("error: hatch binary build produced no output", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
