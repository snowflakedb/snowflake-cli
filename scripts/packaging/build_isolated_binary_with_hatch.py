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


def hatch_install_python(python_tmp_dir: Path, python_version: str) -> bool:
    """Install Python dist into temp dir for bundling."""

    print("🔨 BUILDING PYTHON FROM SOURCE to avoid AVX2 instructions")
    print("🔨 This ensures no pre-compiled AVX2 optimizations")

    import os
    import tarfile
    import tempfile
    import urllib.request

    # Python source download URL
    python_source_url = f"https://www.python.org/ftp/python/3.10.12/Python-3.10.12.tgz"
    python_install_dir = python_tmp_dir / python_version

    print(f"📥 Downloading Python source: {python_source_url}")

    try:
        # Download Python source
        with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp_file:
            urllib.request.urlretrieve(python_source_url, tmp_file.name)

            # Extract source
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)

                with tarfile.open(tmp_file.name, "r:gz") as tar:
                    tar.extractall(path=build_path)

                # Find extracted Python directory
                python_src_dir = next(build_path.glob("Python-*"))

                print(f"🔨 Compiling Python with conservative flags...")

                # Set conservative compilation flags
                conservative_env = os.environ.copy()
                conservative_env[
                    "CFLAGS"
                ] = "-mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe -O2"
                conservative_env[
                    "CXXFLAGS"
                ] = "-mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe -O2"

                # Configure Python build
                configure_cmd = [
                    "./configure",
                    f"--prefix={python_install_dir}",
                    "--enable-optimizations",
                    "--with-lto=no",  # Disable LTO to avoid optimizer adding AVX2
                    "--disable-ipv6",  # Reduce dependencies
                ]

                print(f"🔧 Configuring: {' '.join(configure_cmd)}")
                result = subprocess.run(
                    configure_cmd, cwd=python_src_dir, env=conservative_env
                )
                if result.returncode != 0:
                    print(f"❌ Configure failed")
                    return False

                # Build Python
                make_cmd = ["make", "-j4"]
                print(f"🔨 Building: {' '.join(make_cmd)}")
                result = subprocess.run(
                    make_cmd, cwd=python_src_dir, env=conservative_env
                )
                if result.returncode != 0:
                    print(f"❌ Build failed")
                    return False

                # Install Python
                python_install_dir.mkdir(parents=True, exist_ok=True)
                install_cmd = ["make", "install"]
                print(f"📦 Installing: {' '.join(install_cmd)}")
                result = subprocess.run(
                    install_cmd, cwd=python_src_dir, env=conservative_env
                )
                if result.returncode != 0:
                    print(f"❌ Install failed")
                    return False

        # Create hatch-dist.json metadata
        import json

        hatch_dist_json = python_install_dir / "hatch-dist.json"
        dist_metadata = {
            "name": "cpython",
            "version": python_version,
            "arch": "x86_64",
            "os": "linux",
            "implementation": "cpython",
            "python_path": "bin/python3.10",
            "stdlib_path": "lib/python3.10",
            "site_packages_path": "lib/python3.10/site-packages",
        }

        with open(hatch_dist_json, "w") as f:
            json.dump(dist_metadata, f, indent=2)

        print("✅ Successfully built and installed Python from source")
        print(f"✅ Created hatch-dist.json metadata file")
        return True

    except Exception as e:
        print(f"❌ Failed to build Python from source: {e}")
        print("🔄 Falling back to ancient pre-built Python distribution...")

        # Fallback: Try to use a very old Python build from early 2022 (oldest 3.10)
        ancient_python_url = "https://github.com/indygreg/python-build-standalone/releases/download/20220227/cpython-3.10.2+20220227-x86_64-unknown-linux-gnu-install_only.tar.gz"

        try:
            print(f"📥 Downloading ancient Python: {ancient_python_url}")
            with tempfile.NamedTemporaryFile(
                suffix=".tar.gz", delete=False
            ) as tmp_file:
                urllib.request.urlretrieve(ancient_python_url, tmp_file.name)

                python_install_dir.mkdir(parents=True, exist_ok=True)

                with tarfile.open(tmp_file.name, "r:gz") as tar:
                    tar.extractall(path=python_install_dir)

                print("✅ Successfully installed ancient Python distribution")
                return True

        except Exception as fallback_error:
            print(f"❌ Fallback also failed: {fallback_error}")

            # Last resort: Use standard hatch installation
            print("🔄 Last resort: Using standard hatch Python installation...")
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
        [python_exe, "-m", "pip", "install", "-U", str(PROJECT_ROOT)],
        capture_output=True,
    )
    return not completed_proc.returncode


def setup_conservative_cargo_config():
    """Ensure cargo config is set up for conservative CPU targeting."""
    import shutil

    home_dir = Path.home()
    cargo_dir = home_dir / ".cargo"
    cargo_config_dest = cargo_dir / "config.toml"
    cargo_config_src = PROJECT_ROOT / ".cargo" / "config.toml"

    if cargo_config_src.exists():
        cargo_dir.mkdir(exist_ok=True)
        shutil.copy2(cargo_config_src, cargo_config_dest)
        print(f"✅ Copied conservative cargo config to {cargo_config_dest}")

        # Verify conservative settings are in place
        config_content = cargo_config_dest.read_text()
        if "-avx" in config_content and "-avx2" in config_content:
            print("✅ Conservative CPU targeting verified in cargo config")
        else:
            print("⚠️  WARNING: Conservative settings not found in cargo config!")
            print("Config content preview:")
            print(config_content[:500])
    else:
        print(f"❌ ERROR: {cargo_config_src} not found")


def hatch_build_binary(archive_path: Path, python_path: Path) -> Path | None:
    """Use hatch to build the binary."""
    # Ensure conservative cargo config is in place
    setup_conservative_cargo_config()

    # Set conservative CPU flags to prevent AVX2 instructions in binary build
    import os

    conservative_env = os.environ.copy()

    # Disable modern CPU features in Rust compilation
    rust_flags = "-C target-feature=-avx,-avx2,-avx512f,-fma,-bmi1,-bmi2,-lzcnt,-pclmulqdq,-movbe"
    conservative_env["RUSTFLAGS"] = rust_flags

    # Disable modern CPU features in C compilation
    c_flags = "-mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe"
    conservative_env["CFLAGS"] = c_flags
    conservative_env["CXXFLAGS"] = c_flags

    print("🐛 BINARY BUILD: Setting conservative CPU flags to prevent AVX2 instructions")
    print(f"🐛 RUSTFLAGS: {rust_flags}")

    conservative_env["PYAPP_SKIP_INSTALL"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
    conservative_env["PYAPP_FULL_ISOLATION"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PYTHON_PATH"] = str(python_path)
    conservative_env["PYAPP_DISTRIBUTION_PIP_AVAILABLE"] = "1"

    completed_proc = subprocess.run(
        ["hatch", "build", "-t", "binary"], capture_output=True, env=conservative_env
    )
    if completed_proc.returncode:
        print(completed_proc.stderr)
        return None
    # The binary location is the last line of stderr
    return Path(completed_proc.stderr.decode().split()[-1])


def main():
    settings = ProjectSettings()
    print("Installing Python distribution to TMP dir...")
    hatch_install_python(settings.python_tmp_dir, settings.python_version)
    print("-> installed")

    print(f"Installing project into Python distribution...")
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
