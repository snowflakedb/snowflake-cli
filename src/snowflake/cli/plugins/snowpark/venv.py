import json
import logging
import os
import shutil
import subprocess
import sys
import venv
from enum import Enum
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

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
            log.warning(sys.getdefaultencoding()) # TODO remove this
            process = subprocess.run(
                [self.python_path, *args],
                capture_output=True,
                text=True,
                encoding=sys.getdefaultencoding(),
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
        installed_packages = self._get_installed_packages_metadata()
        dependencies: Dict = {}

        def _get_dependencies(package: Requirement):
            if package.name not in dependencies.keys():
                requires = installed_packages.get(package.name, {}).get(
                    "requires_dist", []
                )
                files = self._parse_file_list(
                    self._get_library_path(),
                    self._parse_info(package.name, PackageInfoType.FILES),
                )

                dependencies[package.name] = RequirementWithFilesAndDeps(
                    requirement=package, files=files, dependencies=requires
                )

                log.warning(  # TODO: change back to debug
                    "Checking package %s, with dependencies: %s", package.name, requires
                )
                log.warning(installed_packages)

                for package in requires:
                    _get_dependencies(Requirement.parse_line(package))

        if req_type == RequirementType.PACKAGE:
            _get_dependencies(Requirement.parse_line(name))

        elif req_type == RequirementType.FILE:
            if Path(name).exists():
                with open(name, "r") as req_file:
                    for line in req_file:
                        _get_dependencies(Requirement.parse_line(line))

        return [dep for dep in dependencies.values()]

    def _get_installed_packages_metadata(self):
        if inspect := self.get_pip_inspect():
            return {
                package.get("metadata", {}).get("name"): package.get("metadata", {})
                for package in inspect.get("installed", {})
            }
        else:
            return {}

    def get_pip_inspect(self) -> Dict:
        result = self.run_python(["-m", "pip", "inspect"])
        log.warning("Result of pip inspect:")
        log.warning(result)  # TODO: remove this calls
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
        log.warning("Result of: print(%s(%s))", info_type.value, package_name)
        log.warning(info)  # TODO remove this call
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
