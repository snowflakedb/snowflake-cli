
import logging
import os
import re
import shutil
import subprocess
import sys
import venv
from enum import Enum
from importlib.metadata import PackagePath
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List, Optional

from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    RequirementType,
    RequirementWithFilesAndDeps,
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
            )
        except subprocess.CalledProcessError as e:
            log.error(self.ERROR_MESSAGE, "python" + " ".join(args), e.stderr)
            raise SystemExit

        return process

    def pip_install(self, name: str, req_type: RequirementType):
        arguments = ["-m", "pip", "install"]
        arguments += ["-r", name] if req_type == RequirementType.FILE else [name]
        process = self.run_python(arguments)

        return process.returncode

    def _create_venv(self):
        venv.create(self.directory.name, self.with_pip)

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
        self, name: str, req_type: RequirementType
    ) -> List[RequirementWithFilesAndDeps]:

        if req_type == RequirementType.PACKAGE:
            dependencies = self._get_dependencies(Requirement.parse_line(name))

        elif req_type == RequirementType.FILE:
            if Path(name).exists():
                with open(name, "r") as req_file:
                    dependencies = [
                        package
                        for line in req_file
                        for package in self._get_dependencies(
                            Requirement.parse_line(line)
                        )
                    ]
            else:
                dependencies = []

        return dependencies

    def _get_dependencies(
        self, package: Requirement, result: List[RequirementWithFilesAndDeps] = None
    ) -> List[RequirementWithFilesAndDeps]:
        if not result:
            result = []
        package_info = self.get_package_info(package)
        if package_info in result:
            return result

        result.append(package_info)

        return result + [dep for pack in package_info.dependencies for dep in self._get_dependencies(Requirement.parse_line(pack), result)]

    def get_package_info(self, package: Requirement) -> RequirementWithFilesAndDeps:
        library_path = self._get_library_path()

        dependencies = self._parse_info(package.name, PackageInfoType.DEPENDENCIES)
        files = self._parse_info(package.name, PackageInfoType.FILES)

        return RequirementWithFilesAndDeps(
            requirement=package,
            files=self._parse_file_list(library_path, files),
            dependencies=dependencies,
        )

    def _parse_info(self, package_name: str, info_type: PackageInfoType) -> List[str]:

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
            if file_path.exists():
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
                os.mkdir(destination_file.parent)
            shutil.copy(file, destination / file.relative_to(library_path))
