from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from snowcli.api.constants import TEMPLATES_PATH
from snowcli.plugins.snowpark.models import SplitRequirements


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


def prepare_app_zip(file_path: Path, temp_dir: str) -> str:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    # TODO: think if no file exceptions are handled correctly
    file_name = file_path.name
    temp_path = temp_dir + "/" + file_name
    shutil.copy(file_path, temp_path)
    return temp_path


def generate_snowpark_coverage_wrapper(
    target_file: str,
    proc_name: str,
    proc_signature: str,
    handler_module: str,
    handler_function: str,
    coverage_reports_stage_path: str,
) -> None:
    """Using a hardcoded template (python_templates/snowpark_coverage.py.jinja), substitutes variables
    and writes out a file.
    The resulting file can be used as the initial handler for the stored proc, and uses the coverage package
    to measure code coverage of the actual stored proc code.
    Afterwards, the handler persists the coverage report to json by executing a query.

    Args:
        target_file (str): _description_
        proc_name (str): _description_
        proc_signature (str): _description_
        handler_module (str): _description_
        handler_function (str): _description_
    """

    environment = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
    template = environment.get_template("snowpark_coverage.py.jinja")
    content = template.render(
        {
            "proc_name": proc_name,
            "proc_signature": proc_signature,
            "handler_module": handler_module,
            "handler_function": handler_function,
            "coverage_reports_stage_path": coverage_reports_stage_path,
        }
    )
    with open(target_file, "w", encoding="utf-8") as output_file:
        output_file.write(content)
