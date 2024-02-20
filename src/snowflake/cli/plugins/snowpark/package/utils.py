from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from snowflake.cli.plugins.snowpark.models import Requirement, SplitRequirements
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.models import SplitRequirements


@dataclass
class LookupResult:
    requirements: SplitRequirements
    name: str

    @property
    def message(self):
        return ""


class InAnaconda(LookupResult):
    @property
    def message(self):
        return f"Package {self.name} is available on the Snowflake anaconda channel."


class RequiresPackages(LookupResult):
    @property
    def message(self):
        return f"""The package {self.name} is supported, but does depend on the
                following Snowflake supported native libraries. You should
                include the following in your packages:
                {get_readable_list_of_requirements(self.requirements.snowflake)}"""


class NotInAnaconda(LookupResult):
    @property
    def message(self):
        return f"""The package {self.name} is avaiable through PIP. You can create a zip using:\n
                snow snowpark package create {self.name} -y"""


class NothingFound(LookupResult):
    @property
    def message(self):
        return f"Lookup for package {self.name} resulted in some error. Please check the package name or try again with -y option"


@dataclass
class CreateResult:
    package_name: str
    file_name: Path = Path()


class CreatedSuccessfully(CreateResult):
    @property
    def message(self):
        return f"Package {self.package_name}.zip created. You can now upload it to a stage (`snow snowpark package upload -f {self.package_name}.zip -s packages`) and reference it in your procedure or function."


def prepare_app_zip(file_path: SecurePath, temp_dir: SecurePath) -> SecurePath:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    # TODO: think if no file exceptions are handled correctly
    file_name = file_path.path.name
    temp_path = temp_dir / file_name
    file_path.copy(temp_path.path)
    return temp_path


def get_readable_list_of_requirements(reqs: List[Requirement]):
    return "\n".join((req.line for req in reqs))
