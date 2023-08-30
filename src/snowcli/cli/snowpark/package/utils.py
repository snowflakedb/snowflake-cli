from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path

from snowcli.utils import SplitRequirements


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
                include the following in your packages: {self.requirements.snowflake}"""


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


class CreationError(CreateResult):
    pass
