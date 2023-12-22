from __future__ import annotations

import os
import shutil
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

templates_path = Path(__file__).parent / "../python_templates"


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

    environment = Environment(loader=FileSystemLoader(templates_path))
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


def create_project_template(
    template_name: str, project_directory: str | None = None
) -> None:
    target = project_directory or os.getcwd()
    shutil.copytree(
        Path(importlib.util.find_spec("templates").origin).parent / template_name,  # type: ignore
        target,
        dirs_exist_ok=True,
    )
