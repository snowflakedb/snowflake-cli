import logging
import subprocess
import sys
import venv
from email.parser import HeaderParser
from os.path import abspath, join
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import List

from requirements.requirement import Requirement
from snowflake.cli.plugins.snowpark.models import RequirementWithFiles

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

    def pip_install(self, name: str, req_type: str, directory: str = ".packages"):
        arguments = ["-m", "pip", "install"]
        arguments += ["-r", name] if req_type == "file" else [name]
        process = self.run_python(arguments)

        return process.returncode

    def _create_venv(self):
        venv.create(self.directory.name, self.with_pip)

    @staticmethod
    def _get_python_path(venv_dir: Path):
        if sys.platform == "win32":
            return venv_dir / "scripts" / "python"
        return venv_dir / "bin" / "python"

    def get_package_dependencies(self, name: str, req_type: str):
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
        result = self.run_python(["-m", "pip", "show", "-f", package.name])
        package_info_dict = dict(HeaderParser().parsestr(result.stdout))

        return RequirementWithFilesAndDeps(
            requirement=package,
            files=[
                abspath(join(self.python_path, file))
                for file in package_info_dict["Files"].split("\n")
            ],
            dependencies=package_info_dict["Requires"].split(","),
        )
