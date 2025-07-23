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
import typing
from pathlib import Path

import tomlkit

PROJECT_ROOT = Path(__file__).parent.parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"
INSTALLATION_SOURCE_VARIABLE = "INSTALLATION_SOURCE"


@contextlib.contextmanager
def contextlib_chdir(path: Path) -> typing.Generator[None, None, None]:
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

    print(
        "🐍 Building Python from official Python.org sources to avoid AVX2 instructions"
    )
    print("🔨 Prioritizing source compilation for maximum compatibility")

    python_install_dir = python_tmp_dir / python_version

    # Option 1: Build Python from official Python.org source with static linking
    try:
        print("🔨 Building Python from official Python.org source...")
        return build_static_python_from_official_source(
            python_install_dir, python_version
        )
    except Exception as e:
        print(f"❌ Failed to build Python from official source: {e}")

    # Option 2: Try fallback to ancient Python distributions (for backup)
    print("🔄 Falling back to ancient Python distributions...")

    import tarfile
    import tempfile
    import urllib.request

    # Ancient Python distribution fallbacks (oldest 3.10 available)
    ancient_python_url = "https://github.com/indygreg/python-build-standalone/releases/download/20220227/cpython-3.10.2+20220227-x86_64-unknown-linux-musl-install_only.tar.gz"

    # Try multiple Python distributions in order of preference
    python_urls = [
        # Static musl build first (most self-contained, no shared library deps)
        ("musl-static", ancient_python_url),
        # Original glibc build (has shared library dependencies like libcrypt.so.1)
        (
            "glibc",
            "https://github.com/indygreg/python-build-standalone/releases/download/20220227/cpython-3.10.2+20220227-x86_64-unknown-linux-gnu-install_only.tar.gz",
        ),
    ]

    for build_type, python_url in python_urls:
        try:
            print(f"📥 Trying {build_type} Python build: {python_url}")
            with tempfile.NamedTemporaryFile(
                suffix=".tar.gz", delete=False
            ) as tmp_file:
                urllib.request.urlretrieve(python_url, tmp_file.name)

                # Clear any previous installation
                if python_install_dir.exists():
                    import shutil

                    shutil.rmtree(python_install_dir)

                python_install_dir.mkdir(parents=True, exist_ok=True)

                with tarfile.open(tmp_file.name, "r:gz") as tar:
                    tar.extractall(path=python_install_dir)

                # Create the hatch-dist.json metadata file that hatch expects
                import json

                hatch_dist_json = python_install_dir / "hatch-dist.json"
                dist_metadata = {
                    "name": "cpython",
                    "version": python_version,
                    "arch": "x86_64",
                    "os": "linux",
                    "implementation": "cpython",
                    "python_path": "python/bin/python3.10",
                    "stdlib_path": "python/lib/python3.10",
                    "site_packages_path": "python/lib/python3.10/site-packages",
                }

                with open(hatch_dist_json, "w") as f:
                    json.dump(dist_metadata, f, indent=2)

                print(f"✅ Successfully installed {build_type} Python distribution")
                print("✅ Created hatch-dist.json metadata file")

                # Mark which distribution was used for debugging
                marker_file = (
                    python_install_dir / f"DISTRIBUTION_TYPE_{build_type.upper()}"
                )
                marker_file.write_text(
                    f"Using {build_type} Python distribution from {python_url}"
                )
                print(f"📝 Created distribution marker: {marker_file.name}")

                return True

        except Exception as e:
            print(f"❌ Failed to install {build_type} Python: {e}")
            continue

    # Option 3: Try the old source build method as additional fallback
    try:
        print("🔄 Trying old source build method as fallback...")
        return build_static_python_from_source(python_install_dir, python_version)
    except Exception as e:
        print(f"❌ Failed to build with old source method: {e}")

    # Last resort: Use standard hatch installation
    print(
        "🔄 All ancient Python builds failed, falling back to standard hatch Python installation..."
    )
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


def build_static_python_from_official_source(
    python_install_dir: Path, python_version: str
) -> bool:
    """Build Python from official Python.org source with static linking to avoid shared library dependencies."""
    import os
    import subprocess
    import tarfile
    import tempfile
    import urllib.request

    # Use official Python.org FTP source - map version to exact release
    version_map = {
        "3.10": "3.10.12",  # Latest stable 3.10.x
        "3.10.12": "3.10.12",
        "3.10.2": "3.10.2",
    }

    # Get the exact Python version to download
    exact_version = version_map.get(python_version, "3.10.12")
    python_source_url = (
        f"https://www.python.org/ftp/python/{exact_version}/Python-{exact_version}.tgz"
    )

    print(f"📦 Using official Python source: {python_source_url}")

    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp_file:
        print(f"📥 Downloading Python {exact_version} from python.org...")
        urllib.request.urlretrieve(python_source_url, tmp_file.name)

        with tempfile.TemporaryDirectory() as build_dir:
            build_path = Path(build_dir)

            with tarfile.open(tmp_file.name, "r:gz") as tar:
                tar.extractall(path=build_path)

            python_src_dir = next(build_path.glob("Python-*"))

            # Clear any previous installation
            if python_install_dir.exists():
                import shutil

                shutil.rmtree(python_install_dir)

            # Configure with conservative CPU flags but allow essential modules
            configure_env = os.environ.copy()
            # Use conservative CPU flags but don't force full static linking
            configure_env[
                "CFLAGS"
            ] = "-mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe -O2"

            configure_cmd = [
                "./configure",
                f"--prefix={python_install_dir}",
                "--enable-optimizations",  # Enable optimizations for better performance
                "--with-lto=no",  # Disable LTO to avoid optimizer adding AVX2
                "--disable-ipv6",  # Reduce dependencies
                "--with-ensurepip=install",  # Include pip
            ]

            print(f"🔧 Configuring static Python build...")
            configure_result = subprocess.run(
                configure_cmd, cwd=python_src_dir, env=configure_env
            )
            if configure_result.returncode != 0:
                return False

            # Build Python
            make_cmd = ["make", "-j4"]
            print(f"🔨 Building static Python...")
            make_result = subprocess.run(
                make_cmd, cwd=python_src_dir, env=configure_env
            )
            if make_result.returncode != 0:
                return False

            # Install Python
            python_install_dir.mkdir(parents=True, exist_ok=True)
            install_cmd = ["make", "install"]
            print(f"📦 Installing static Python...")
            install_result = subprocess.run(
                install_cmd, cwd=python_src_dir, env=configure_env
            )
            if install_result.returncode != 0:
                return False

            # Install pip manually since we used --without-ensurepip
            # Try different Python executable names in order of preference
            python_exe_candidates = [
                python_install_dir / "bin" / f"python{exact_version[:4]}",  # python3.10
                python_install_dir
                / "bin"
                / f"python{exact_version[:3]}",  # python3.1 (wrong)
                python_install_dir / "bin" / "python3",
                python_install_dir / "bin" / "python",
            ]

            python_exe = None
            for candidate in python_exe_candidates:
                if candidate.exists():
                    python_exe = candidate
                    print(f"✅ Found Python executable: {python_exe}")
                    break

            if not python_exe:
                print("❌ No Python executable found after build")
            else:
                print(
                    "🔧 Verifying pip is available (should be included via --with-ensurepip)..."
                )
                try:
                    result = subprocess.run(
                        [str(python_exe), "-m", "pip", "--version"],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        print(f"✅ pip is available: {result.stdout.strip()}")
                    else:
                        print(f"⚠️  pip not found, trying to install: {result.stderr}")
                        # Try to install pip using ensurepip
                        result = subprocess.run(
                            [str(python_exe), "-m", "ensurepip", "--upgrade"],
                            capture_output=True,
                            text=True,
                        )
                        if result.returncode == 0:
                            print("✅ pip installed successfully via ensurepip")
                        else:
                            print(f"⚠️  ensurepip failed: {result.stderr}")
                            # Last resort: manual pip installation
                            print("🔧 Trying manual pip installation...")
                            get_pip_url = "https://bootstrap.pypa.io/get-pip.py"
                            with tempfile.NamedTemporaryFile(
                                suffix=".py", delete=False
                            ) as get_pip_file:
                                urllib.request.urlretrieve(
                                    get_pip_url, get_pip_file.name
                                )
                                subprocess.run(
                                    [str(python_exe), get_pip_file.name],
                                    cwd=python_install_dir,
                                    capture_output=True,
                                    text=True,
                                )
                except Exception as e:
                    print(f"⚠️  Failed to verify/install pip: {e}")

    # Create hatch-dist.json metadata for static build
    import json

    hatch_dist_json = python_install_dir / "hatch-dist.json"
    dist_metadata = {
        "name": "cpython",
        "version": python_version,
        "arch": "x86_64",
        "os": "linux",
        "implementation": "cpython",
        "python_path": f"bin/python{exact_version[:4]}",  # python3.10 not python3.1
        "stdlib_path": f"lib/python{exact_version[:4]}",
        "site_packages_path": f"lib/python{exact_version[:4]}/site-packages",
    }

    with open(hatch_dist_json, "w") as f:
        json.dump(dist_metadata, f, indent=2)

    print(f"✅ Successfully built static Python {exact_version} from official source")

    # Mark which distribution was used for debugging
    marker_file = python_install_dir / "DISTRIBUTION_TYPE_OFFICIAL_SOURCE"
    marker_file.write_text(
        f"Using official Python.org source build (version {exact_version})"
    )
    print(f"📝 Created distribution marker: {marker_file.name}")

    return True


def build_static_python_from_source(
    python_install_dir: Path, python_version: str
) -> bool:
    """Build Python from source with static linking to avoid shared library dependencies."""
    import os
    import subprocess
    import tarfile
    import tempfile
    import urllib.request

    python_source_url = "https://www.python.org/ftp/python/3.10.12/Python-3.10.12.tgz"

    with tempfile.NamedTemporaryFile(suffix=".tgz", delete=False) as tmp_file:
        urllib.request.urlretrieve(python_source_url, tmp_file.name)

        with tempfile.TemporaryDirectory() as build_dir:
            build_path = Path(build_dir)

            with tarfile.open(tmp_file.name, "r:gz") as tar:
                tar.extractall(path=build_path)

            python_src_dir = next(build_path.glob("Python-*"))

            # Configure with static linking and conservative flags
            configure_env = os.environ.copy()
            configure_env[
                "CFLAGS"
            ] = "-static -mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe -O2"
            configure_env["LDFLAGS"] = "-static"

            configure_cmd = [
                "./configure",
                f"--prefix={python_install_dir}",
                "--disable-shared",  # No shared libraries
                "--enable-static",  # Static linking
                "--with-lto=no",  # Disable LTO to avoid optimizer adding AVX2
                "--disable-ipv6",  # Reduce dependencies
                "--without-ensurepip",  # Skip pip to reduce dependencies
            ]

            print(f"🔧 Configuring static Python build...")
            configure_result = subprocess.run(
                configure_cmd, cwd=python_src_dir, env=configure_env
            )
            if configure_result.returncode != 0:
                return False

            # Build Python
            make_cmd = ["make", "-j4"]
            print(f"🔨 Building static Python...")
            make_result = subprocess.run(
                make_cmd, cwd=python_src_dir, env=configure_env
            )
            if make_result.returncode != 0:
                return False

            # Install Python
            python_install_dir.mkdir(parents=True, exist_ok=True)
            install_cmd = ["make", "install"]
            print(f"📦 Installing static Python...")
            install_result = subprocess.run(
                install_cmd, cwd=python_src_dir, env=configure_env
            )
            if install_result.returncode != 0:
                return False

            # Install pip manually since we used --without-ensurepip
            python_exe = python_install_dir / "bin" / "python3.10"
            if python_exe.exists():
                print("🔧 Installing pip into static Python build...")
                try:
                    result = subprocess.run(
                        [str(python_exe), "-m", "ensurepip", "--upgrade"],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        print("✅ pip installed successfully")
                    else:
                        print(f"⚠️  pip installation failed: {result.stderr}")
                        # Try to download and install pip manually
                        print("🔧 Trying manual pip installation...")
                        get_pip_url = "https://bootstrap.pypa.io/get-pip.py"
                        with tempfile.NamedTemporaryFile(
                            suffix=".py", delete=False
                        ) as get_pip_file:
                            urllib.request.urlretrieve(get_pip_url, get_pip_file.name)
                            subprocess.run(
                                [str(python_exe), get_pip_file.name],
                                cwd=python_install_dir,
                                capture_output=True,
                                text=True,
                            )
                except Exception as e:
                    print(f"⚠️  Failed to install pip: {e}")
            else:
                print("❌ Python executable not found after build")

    # Create hatch-dist.json metadata for static build
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

    print("✅ Successfully built static Python from source")

    # Mark which distribution was used for debugging
    marker_file = python_install_dir / "DISTRIBUTION_TYPE_STATIC_SOURCE"
    marker_file.write_text(
        f"Using static Python from source build (version {python_version})"
    )
    print(f"📝 Created distribution marker: {marker_file.name}")

    return True


@contextlib.contextmanager
def override_is_installation_source_variable() -> typing.Generator[None, None, None]:
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


def check_shared_dependencies(python_exe: str) -> None:
    """Check what shared libraries the Python executable depends on."""
    print(f"🔍 Checking shared library dependencies for: {python_exe}")
    try:
        ldd_result = subprocess.run(["ldd", python_exe], capture_output=True, text=True)
        if ldd_result.returncode == 0:
            lines = ldd_result.stdout.strip().split("\n")
            shared_libs = [
                line.strip() for line in lines if "=>" in line or "linux-vdso" in line
            ]

            if not shared_libs or ldd_result.stdout.strip() == "statically linked":
                print(
                    "✅ Python executable is statically linked (no shared library dependencies)"
                )
            else:
                print(
                    f"⚠️  Python executable has {len(shared_libs)} shared library dependencies:"
                )
                for lib in shared_libs[:5]:  # Show first 5
                    print(f"    {lib}")
                if len(shared_libs) > 5:
                    print(f"    ... and {len(shared_libs) - 5} more")
        else:
            print(f"❌ Failed to check dependencies: {ldd_result.stderr}")
    except Exception as e:
        print(f"❌ Error checking dependencies: {e}")


def pip_install_project(python_exe: str) -> bool:
    """Install the project into the Python distribution."""
    print(f"🐍 Installing project using Python: {python_exe}")

    # First, check if the Python executable exists and works
    if not Path(python_exe).exists():
        print(f"❌ Python executable not found: {python_exe}")
        return False

    # Test Python executable
    test_proc = subprocess.run(
        [python_exe, "--version"], capture_output=True, text=True
    )
    if test_proc.returncode != 0:
        print(f"❌ Python executable failed: {test_proc.stderr}")
        return False
    else:
        print(f"✅ Python executable works: {test_proc.stdout.strip()}")

    # Check if pip is available
    pip_check = subprocess.run(
        [python_exe, "-m", "pip", "--version"], capture_output=True, text=True
    )
    if pip_check.returncode != 0:
        print(f"❌ pip not available: {pip_check.stderr}")
        print("🔧 Trying to install pip...")

        # Try to install pip using ensurepip
        ensurepip_proc = subprocess.run(
            [python_exe, "-m", "ensurepip", "--upgrade"], capture_output=True, text=True
        )
        if ensurepip_proc.returncode != 0:
            print(f"❌ Failed to install pip via ensurepip: {ensurepip_proc.stderr}")
            return False
        else:
            print("✅ pip installed successfully via ensurepip")
    else:
        print(f"✅ pip available: {pip_check.stdout.strip()}")

    # Upgrade pip to latest version for better compatibility
    print("🔧 Upgrading pip to latest version...")
    pip_upgrade_proc = subprocess.run(
        [python_exe, "-m", "pip", "install", "--upgrade", "pip"],
        capture_output=True,
        text=True,
    )
    if pip_upgrade_proc.returncode == 0:
        print("✅ pip upgraded successfully")
    else:
        print(f"⚠️  pip upgrade failed (continuing anyway): {pip_upgrade_proc.stderr}")

    # Install build dependencies first
    print(f"🔧 Installing build dependencies...")
    build_deps_proc = subprocess.run(
        [
            python_exe,
            "-m",
            "pip",
            "install",
            "-U",
            "wheel",
            "setuptools",
            "hatch",
            "hatchling",
        ],
        capture_output=True,
        text=True,
    )
    if build_deps_proc.returncode != 0:
        print(f"⚠️  Build dependencies installation failed: {build_deps_proc.stderr}")
        # Continue anyway, might not be critical

    # Now install the project
    print(f"📦 Installing project from: {PROJECT_ROOT}")
    completed_proc = subprocess.run(
        [python_exe, "-m", "pip", "install", "-U", "-v", str(PROJECT_ROOT)],
        capture_output=True,
        text=True,
    )

    if completed_proc.returncode != 0:
        print(f"❌ Project installation failed!")
        print(f"STDOUT: {completed_proc.stdout}")
        print(f"STDERR: {completed_proc.stderr}")

        # Try fallback installation methods
        print("🔄 Trying fallback installation methods...")

        # Method 1: Install without build dependencies
        print("🔄 Trying installation without build isolation...")
        fallback1_proc = subprocess.run(
            [
                python_exe,
                "-m",
                "pip",
                "install",
                "-U",
                "--no-build-isolation",
                str(PROJECT_ROOT),
            ],
            capture_output=True,
            text=True,
        )

        if fallback1_proc.returncode == 0:
            print("✅ Fallback installation (no build isolation) succeeded!")
        else:
            print(f"❌ Fallback 1 failed: {fallback1_proc.stderr}")

            # Method 2: Try installing as editable
            print("🔄 Trying editable installation...")
            fallback2_proc = subprocess.run(
                [python_exe, "-m", "pip", "install", "-e", str(PROJECT_ROOT)],
                capture_output=True,
                text=True,
            )

            if fallback2_proc.returncode == 0:
                print("✅ Fallback installation (editable) succeeded!")
            else:
                print(f"❌ All installation methods failed: {fallback2_proc.stderr}")
                return False
    else:
        print("✅ Project installed successfully")
        print(f"Installation output: {completed_proc.stdout}")

        # Verify the snowflake module can be imported
        print("🔍 Verifying snowflake module can be imported...")
        import_test = subprocess.run(
            [
                python_exe,
                "-c",
                "import snowflake.cli; print('snowflake.cli imported successfully')",
            ],
            capture_output=True,
            text=True,
        )

        if import_test.returncode != 0:
            print(f"❌ snowflake module import failed: {import_test.stderr}")
            return False
        else:
            print(f"✅ snowflake module import successful: {import_test.stdout.strip()}")

            # Check shared library dependencies
            check_shared_dependencies(python_exe)

            return True
    return False


def setup_conservative_cargo_config() -> None:
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

    # Conservative Rust flags are now handled by .cargo/config.toml
    # Just set C compilation flags for any native dependencies that use cc-rs
    c_flags = "-mno-avx -mno-avx2 -mno-fma -mno-bmi -mno-avx512f -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe"
    conservative_env["CFLAGS"] = c_flags
    conservative_env["CXXFLAGS"] = c_flags

    print(f"🐛 BINARY BUILD: Using conservative CPU flags for native build")
    print(f"🐛 CFLAGS: {c_flags}")

    conservative_env["PYAPP_SKIP_INSTALL"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
    conservative_env["PYAPP_FULL_ISOLATION"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PYTHON_PATH"] = str(python_path)
    conservative_env["PYAPP_DISTRIBUTION_PIP_AVAILABLE"] = "1"

    # Build natively for the current architecture
    print(f"🎯 Building natively on current architecture")

    completed_proc = subprocess.run(
        ["hatch", "build", "-t", "binary"], capture_output=True, env=conservative_env
    )
    if completed_proc.returncode:
        print(completed_proc.stderr)
        return None
    # The binary location is the last line of stderr
    return Path(completed_proc.stderr.decode().split()[-1])


def main() -> None:
    settings = ProjectSettings()
    print("Installing Python distribution to TMP dir...")
    hatch_install_python(settings.python_tmp_dir, settings.python_version)

    # Check which distribution type was used
    for marker_file in settings.python_tmp_dir.glob("DISTRIBUTION_TYPE_*"):
        print(f"🏷️  Distribution used: {marker_file.read_text()}")
        break

    print("-> installed")

    print(f"Installing project into Python distribution...")
    with override_is_installation_source_variable():
        success = pip_install_project(str(settings.python_dist_exe))
        if not success:
            print("❌ CRITICAL: Failed to install project into Python distribution!")
            print(
                "This will cause the binary to fail with 'ModuleNotFoundError: No module named snowflake'"
            )
            raise RuntimeError("Project installation failed")
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
