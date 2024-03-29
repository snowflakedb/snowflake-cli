import json
import locale
import logging
import os
import shutil
import subprocess
import sys
import venv
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional

from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    RequirementWithWheelAndDeps,
    WheelMetadata,
)

log = logging.getLogger(__name__)


class PackageInfoType(Enum):
    FILES = "files"
    DEPENDENCIES = "requires"


class Venv:
    ERROR_MESSAGE = "Running command {0} caused error {1}"

    def __init__(self, directory: str = "", with_pip: bool = True):
        self.directory = TemporaryDirectory(directory)
        self.with_pip = with_pip

    def __enter__(self):
        self._create_venv()
        self.python_path = self._get_python_path(Path(self.directory.name))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.directory.cleanup()

    def run_python(self, args):

        try:
            process = subprocess.run(
                [self.python_path, *args],
                capture_output=True,
                text=True,
                encoding=locale.getpreferredencoding(),
            )
        except subprocess.CalledProcessError as e:
            log.error(self.ERROR_MESSAGE, "python" + " ".join(args), e.stderr)
            raise SystemExit

        return process

    def pip_install(self, requirements_file):
        process = self.run_python(["-m", "pip", "install", "-r", requirements_file])
        return process.returncode

    def pip_wheel(
        self,
        requirements_file: Optional[str],
        package_name: Optional[str],
        download_dir: Path,
        index_url: Optional[str],
        dependencies: bool = True,
    ):
        command = ["-m", "pip", "wheel", "-w", download_dir]
        if package_name:
            command.append(package_name)
        if requirements_file:
            command += ["-r", requirements_file]
        if index_url is not None:
            command += ["-i", index_url]
        if not dependencies:
            command += ["--no-deps"]
        process = self.run_python(command)
        return process.returncode

    def _create_venv(self):
        venv.create(self.directory.name, with_pip=self.with_pip)

    @staticmethod
    def _get_python_path(venv_dir: Path) -> Path:
        if sys.platform == "win32":
            return venv_dir / "scripts" / "python"
        return venv_dir / "bin" / "python"

    def _get_library_path(self) -> Path:
        return [
            lib for lib in (Path(self.directory.name) / "lib").glob("**/site-packages")
        ][0]

    def get_package_dependencies(
        self, requirements_file: SecurePath, downloads_dir: Path
    ) -> List[RequirementWithWheelAndDeps]:
        packages_metadata: Dict[str, WheelMetadata] = {
            meta.name: meta
            for meta in (
                WheelMetadata.from_wheel(wheel_path)
                for wheel_path in downloads_dir.glob("*.whl")
            )
            if meta is not None
        }
        dependencies: Dict = {}

        def _get_dependencies(package: Requirement):
            if package.name not in dependencies:
                meta = packages_metadata.get(
                    WheelMetadata.to_wheel_name_format(package.name)
                )
                wheel_path = meta.wheel_path if meta else None
                requires = meta.dependencies if meta else []
                dependencies[package.name] = RequirementWithWheelAndDeps(
                    requirement=package,
                    wheel_path=wheel_path,
                    dependencies=requires,
                )

                log.debug(
                    "Checking package %s, with dependencies: %s", package.name, requires
                )

                for package in requires:
                    _get_dependencies(Requirement.parse_line(package))

        with requirements_file.open("r", read_file_limit_mb=512) as req_file:
            for line in req_file:
                _get_dependencies(Requirement.parse_line(line))

        return list(dependencies.values())

    def _get_installed_packages_metadata(self):
        if inspect := self.get_pip_inspect():
            return {
                package.get("metadata", {}).get("name"): package.get("metadata", {})
                for package in inspect.get("installed", {})
            }
        else:
            return {}

    def get_pip_inspect(self) -> Dict:
        result = self.run_python(["-m", "pip", "inspect", "--local"])

        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            return {}

    def _parse_info(self, package_name: str, info_type: PackageInfoType) -> List[str]:
        from importlib.metadata import PackagePath  # noqa

        info = self.run_python(
            [
                "-c",
                f"from importlib.metadata import {info_type.value}; print({info_type.value}('{package_name}'))",
            ]
        )
        if info.returncode != 0:
            return []

        result = eval(info.stdout)

        if result:
            return result
        else:
            return []

    def _parse_file_list(self, base_dir: Path, files: List):
        result = []

        for file in [Path(f) for f in files]:
            file_path = base_dir / file
            if file_path.exists() and not ".." in file_path.parts:
                result.append(str(file_path))

        return result

    def copy_files_to_packages_dir(
        self, files_to_be_copied: List[Path], destination: Path = Path(".packages")
    ) -> None:
        if not destination.exists():
            destination.mkdir()
        library_path = self._get_library_path()

        for file in files_to_be_copied:
            destination_file = destination / file.relative_to(library_path)

            if not destination_file.parent.exists():
                os.makedirs(destination_file.parent)
            shutil.copy(file, destination / file.relative_to(library_path))
