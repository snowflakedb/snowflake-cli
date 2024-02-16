import logging
import os
import re
import shutil
import subprocess
import sys
import venv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List

from snowflake.cli.plugins.snowpark.models import (
    Requirement,
    RequirementWithFilesAndDeps,
)

log = logging.getLogger(__name__)


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

    def pip_install(self, name: str, req_type: str):
        arguments = ["-m", "pip", "install"]
        arguments += ["-r", name] if req_type == "file" else [name]
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
        directory = os.listdir(Path(self.directory.name) / "lib")[0]
        return (
            Path(self.directory.name) / "lib" / directory / "site-packages"
        ).absolute()

    def get_package_dependencies(
        self, name: str, req_type: str
    ) -> List[RequirementWithFilesAndDeps]:

        if req_type == "package":
            dependencies = self._get_dependencies(Requirement.parse_line(name))

        elif req_type == "file":
            with open(name, "r") as req_file:
                dependencies = [
                    package
                    for line in req_file
                    for package in self._get_dependencies(Requirement.parse_line(line))
                ]

        return dependencies

    def _get_dependencies(
        self, package: Requirement
    ) -> List[RequirementWithFilesAndDeps]:
        package_info = self.get_package_info(package)
        result = [package_info]

        for dependency in package_info.dependencies:
            if dependency:
                result += self._get_dependencies(Requirement.parse_line(dependency))

        return result

    def get_package_info(self, package: Requirement) -> RequirementWithFilesAndDeps:
        library_path = self._get_library_path()
        result = self.run_python(["-m", "pip", "show", "-f", package.name])
        package_info_dict = self._parse_pip_info(result.stdout)

        return RequirementWithFilesAndDeps(
            requirement=package,
            files=self._parse_file_list(
                library_path, package_info_dict.get("Files", [])
            ),
            dependencies=package_info_dict.get("Requires", []),
        )

    def _parse_pip_info(self, pip_string: str) -> Dict:
        pattern = "^{}:(.*)$"
        matchers = [
            ("Name", re.MULTILINE, ""),
            ("Requires", re.MULTILINE, ","),
            ("Files", re.MULTILINE | re.DOTALL, "\n"),
        ]
        info_dict = {}

        for matcher in matchers:
            result = re.search(
                pattern.format(matcher[0]), string=pip_string, flags=matcher[1]
            )
            if result:
                result_string = result.group(0).replace(f"{matcher[0]}:", "").strip()
                info_dict[matcher[0]] = (
                    result_string.split(matcher[2]) if matcher[2] else result_string
                )

        return info_dict

    def _parse_file_list(self, base_dir: Path, files: List):
        result = []

        for file in files:
            file_path = base_dir / file.strip()
            if file_path.exists():
                result.append(str(file_path))

        return result

    def copy_files_to_packages_dir(
        self, files_to_be_copied: List[Path], destination: Path = Path(".packages")
    ) -> None:
        if not destination.exists():
            destination.mkdir()
        library_path = self._get_library_path()
        src_directories = set(
            file.relative_to(library_path).parts[0] for file in files_to_be_copied
        )

        for src_dir in src_directories:
            shutil.copytree(
                library_path / src_dir, destination / src_dir, dirs_exist_ok=True
            )
