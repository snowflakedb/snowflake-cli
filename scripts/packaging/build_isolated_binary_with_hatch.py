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
