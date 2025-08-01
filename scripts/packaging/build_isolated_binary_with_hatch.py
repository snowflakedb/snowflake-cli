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

    print("🚀 FORCING Conservative Source Build to eliminate CPU optimizations")
    print(
        "🔨 Skipping pre-built distributions - building from source with conservative CPU flags"
    )

    python_install_dir = python_tmp_dir / python_version

    # Clear any previous installation
    if python_install_dir.exists():
        import shutil

        shutil.rmtree(python_install_dir)

    # Option 1: Try conservative source build method (with conservative CPU flags)
    try:
        print("🔨 Building Python from source with conservative CPU flags...")
        print("⚡ This will disable: AVX, AVX2, FMA, BMI, AVX512, LZCNT, PCLMUL, MOVBE")
        if build_static_python_from_source(python_install_dir, python_version):
            print("✅ Conservative source build completed successfully!")
            return True
    except Exception as e:
        print(f"❌ Failed conservative source build: {e}")

    # Option 2: Try official Python.org source build as fallback
    try:
        print("🔨 Trying official Python.org source build as fallback...")
        if build_static_python_from_official_source(python_install_dir, python_version):
            print("✅ Conservative Python build completed successfully!")
            return True
    except Exception as e:
        print(f"❌ Failed official source build: {e}")

    # Last resort: Use standard hatch installation (may still have optimizations)
    print(
        "⚠️  WARNING: Falling back to standard hatch installation - may contain CPU optimizations!"
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

            # Configure with static linking and conservative flags
            configure_env = os.environ.copy()
            configure_env[
                "CFLAGS"
            ] = "-static -mno-avx -mno-avx2 -mno-avx512f -mno-avx512cd -mno-avx512dq -mno-avx512bw -mno-avx512vl -mno-avx512ifma -mno-avx512vbmi -mno-avx512vbmi2 -mno-avx512vnni -mno-avx512bitalg -mno-avx512vpopcntdq -mno-fma -mno-bmi -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe -O2"
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
            python_exe = python_install_dir / "bin" / f"python{exact_version[:4]}"
            if not python_exe.exists():
                python_exe = python_install_dir / "bin" / "python3"
            if not python_exe.exists():
                python_exe = python_install_dir / "bin" / "python"

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
        "python_path": f"bin/python{exact_version[:4]}",
        "stdlib_path": f"lib/python{exact_version[:4]}",
        "site_packages_path": f"lib/python{exact_version[:4]}/site-packages",
    }

    with open(hatch_dist_json, "w") as f:
        json.dump(dist_metadata, f, indent=2)

    print(
        f"✅ Successfully built conservative Python {exact_version} from official source"
    )

    # Mark which distribution was used for debugging
    marker_file = python_install_dir / "DISTRIBUTION_TYPE_CONSERVATIVE_BUILD"
    marker_file.write_text(
        f"Using conservative Python.org source build (version {exact_version})"
    )
    print(f"📝 Created distribution marker: {marker_file.name}")

    return True


def build_static_python_from_source(
    python_install_dir: Path, python_version: str
) -> bool:
    """Build Python from source with conservative CPU flags to avoid illegal instructions."""
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

            # Modify Modules/Setup to ensure critical modules are built
            setup_path = python_src_dir / "Modules" / "Setup"
            if setup_path.exists():
                print(f"📝 Modifying Modules/Setup to enable essential modules...")
                with open(setup_path, "r") as f:
                    setup_content = f.read()

                # Uncomment essential modules by removing leading #
                essential_modules = [
                    "#_posixsubprocess _posixsubprocess.c",
                    "#_subprocess _subprocess.c",
                    "#array arraymodule.c",
                    "#math mathmodule.c",
                    "#_struct _struct.c",
                    "#time timemodule.c",
                    "#select selectmodule.c",
                    "#_socket socketmodule.c",
                    "#binascii binascii.c",
                    "#unicodedata unicodedata.c",
                    "#_datetime _datetimemodule.c",
                    "#_random _randommodule.c",
                    "#_pickle _pickle.c",
                    "#_json _json.c",
                ]

                for module_line in essential_modules:
                    if module_line in setup_content:
                        setup_content = setup_content.replace(
                            module_line, module_line[1:]
                        )  # Remove #
                        print(f"  ✅ Enabled: {module_line[1:].split()[0]}")

                with open(setup_path, "w") as f:
                    f.write(setup_content)
                print(f"✅ Modified Setup to enable essential modules")
            else:
                print(f"⚠️  Modules/Setup not found, using Setup.local fallback")
                # Fallback to Setup.local
                setup_local_path = python_src_dir / "Modules" / "Setup.local"
                setup_local_content = """
_posixsubprocess _posixsubprocess.c
_subprocess _subprocess.c
array arraymodule.c
math mathmodule.c
_struct _struct.c
time timemodule.c
select selectmodule.c
_socket socketmodule.c
binascii binascii.c
unicodedata unicodedata.c
_datetime _datetimemodule.c
_random _randommodule.c
_pickle _pickle.c
_json _json.c
"""
                with open(setup_local_path, "w") as f:
                    f.write(setup_local_content)

            # Configure with conservative CPU flags but allow modules to build properly
            configure_env = os.environ.copy()
            # Use conservative CPU flags but keep them reasonable for module building
            configure_env["CFLAGS"] = "-O2 -mno-avx -mno-avx2 -mno-avx512f -mno-fma"
            configure_env["CXXFLAGS"] = "-O2 -mno-avx -mno-avx2 -mno-avx512f -mno-fma"

            configure_cmd = [
                "./configure",
                f"--prefix={python_install_dir}",
                "--with-lto=no",  # Disable LTO to avoid optimizer adding AVX2
                "--disable-ipv6",  # Reduce dependencies
                "--with-ensurepip=install",  # Include pip - needed for project installation
                "--without-readline",  # Avoid readline dependencies
                "--enable-loadable-sqlite-extensions",  # Enable sqlite
                "--with-computed-gotos",  # Performance optimization
                "--enable-shared",  # Allow shared libraries for extension modules
                "--enable-optimizations",  # Enable optimizations but with our conservative CFLAGS
            ]

            print(f"🔧 Configuring conservative Python build with essential modules...")
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
            print(f"📦 Installing conservative Python...")
            install_result = subprocess.run(
                install_cmd, cwd=python_src_dir, env=configure_env
            )
            if install_result.returncode != 0:
                return False

            # Verify Python executable and pip are available (pip included via --with-ensurepip=install)
            python_exe = python_install_dir / "bin" / "python3.10"
            if not python_exe.exists():
                python_exe = python_install_dir / "bin" / "python3"
            if not python_exe.exists():
                python_exe = python_install_dir / "bin" / "python"

            if python_exe.exists():
                print("✅ Python executable found, verifying pip availability...")
                try:
                    result = subprocess.run(
                        [str(python_exe), "-c", "import pip; print('pip available')"],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        print("✅ pip is available in the built Python")
                    else:
                        print(f"⚠️  pip verification failed: {result.stderr}")
                except Exception as e:
                    print(f"⚠️  pip verification error: {e}")
            else:
                print("❌ Python executable not found after build")
                return False

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

    print("✅ Successfully built conservative Python from source")

    # Mark which distribution was used for debugging
    marker_file = python_install_dir / "DISTRIBUTION_TYPE_CONSERVATIVE_SOURCE"
    marker_file.write_text(
        f"Using conservative Python from source build (version {python_version}) - no AVX/AVX2/FMA instructions"
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

    # Set up environment with library path for shared libraries
    import os

    python_path = Path(python_exe)
    python_install_dir = python_path.parent.parent  # ../bin/python -> ..
    lib_dir = python_install_dir / "lib"

    # Create environment with LD_LIBRARY_PATH for shared library resolution
    run_env = os.environ.copy()
    if lib_dir.exists():
        current_ld_path = run_env.get("LD_LIBRARY_PATH", "")
        if current_ld_path:
            run_env["LD_LIBRARY_PATH"] = f"{lib_dir}:{current_ld_path}"
        else:
            run_env["LD_LIBRARY_PATH"] = str(lib_dir)
        print(f"🔧 Set LD_LIBRARY_PATH to include: {lib_dir}")
    else:
        print(f"⚠️  Library directory not found: {lib_dir}")

    # Test Python executable with proper library path
    test_proc = subprocess.run(
        [python_exe, "--version"], capture_output=True, text=True, env=run_env
    )
    if test_proc.returncode != 0:
        print(f"❌ Python executable failed: {test_proc.stderr}")
        return False
    else:
        print(f"✅ Python executable works: {test_proc.stdout.strip()}")

    # Check if pip is available
    pip_check = subprocess.run(
        [python_exe, "-m", "pip", "--version"],
        capture_output=True,
        text=True,
        env=run_env,
    )
    if pip_check.returncode != 0:
        print(f"❌ pip not available: {pip_check.stderr}")
        print("🔧 Trying to install pip...")

        # Try to install pip using ensurepip
        ensurepip_proc = subprocess.run(
            [python_exe, "-m", "ensurepip", "--upgrade"],
            capture_output=True,
            text=True,
            env=run_env,
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
        env=run_env,
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
        env=run_env,
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
        env=run_env,
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
            env=run_env,
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
                env=run_env,
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
            env=run_env,
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


def hatch_build_binary(
    archive_path: Path, python_path: Path
) -> typing.Union[Path, None]:
    """Use hatch to build the binary."""
    # Ensure conservative cargo config is in place
    setup_conservative_cargo_config()

    # Set conservative CPU flags to prevent AVX2 instructions in binary build
    import os

    conservative_env = os.environ.copy()

    # Conservative Rust flags are now handled by .cargo/config.toml
    # Just set C compilation flags for any native dependencies that use cc-rs
    c_flags = "-mno-avx -mno-avx2 -mno-avx512f -mno-avx512cd -mno-avx512dq -mno-avx512bw -mno-avx512vl -mno-avx512ifma -mno-avx512vbmi -mno-avx512vbmi2 -mno-avx512vnni -mno-avx512bitalg -mno-avx512vpopcntdq -mno-fma -mno-bmi -mno-bmi2 -mno-lzcnt -mno-pclmul -mno-movbe"
    conservative_env["CFLAGS"] = c_flags
    conservative_env["CXXFLAGS"] = c_flags

    print(f"🐛 BINARY BUILD: Using conservative CPU flags for native build")
    print(f"🐛 CFLAGS: {c_flags}")

    conservative_env["PYAPP_SKIP_INSTALL"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PATH"] = str(archive_path)
    conservative_env["PYAPP_FULL_ISOLATION"] = "1"
    conservative_env["PYAPP_DISTRIBUTION_PYTHON_PATH"] = str(python_path)
    conservative_env["PYAPP_DISTRIBUTION_PIP_AVAILABLE"] = "1"

    # Force static linking for PyApp binary
    conservative_env[
        "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS"
    ] = "-C target-feature=+crt-static"
    conservative_env["RUSTFLAGS"] = "-C target-feature=+crt-static"
    conservative_env["CC"] = "musl-gcc"

    # Build statically for x86_64 using musl for better static linking
    print(f"🎯 Building static binary for x86_64-unknown-linux-musl")

    # Add musl target if not already added
    musl_target_cmd = subprocess.run(
        ["rustup", "target", "add", "x86_64-unknown-linux-musl"], capture_output=True
    )

    conservative_env["CARGO_BUILD_TARGET"] = "x86_64-unknown-linux-musl"
    print(
        f"🔧 STATIC BUILD: Targeting x86_64-unknown-linux-musl for full static linking"
    )

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
