from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

from jinja2 import Environment, FileSystemLoader
from snowflake.cli.api.constants import TEMPLATES_PATH
from snowflake.cli.plugins.snowpark.package_utils import parse_requirements

log = logging.getLogger(__name__)


def generate_streamlit_environment_file(
    excluded_anaconda_deps: Optional[List[str]],
    requirements_file: str = "requirements.snowflake.txt",
) -> Optional[Path]:
    """Creates an environment.yml file for streamlit deployment, if a Snowflake
    requirements file exists.
    The file path is returned if it was generated, otherwise None is returned.
    """
    if Path(requirements_file).exists():
        snowflake_requirements = parse_requirements(requirements_file)

        # remove explicitly excluded anaconda dependencies
        if excluded_anaconda_deps is not None:
            log.info("Excluded dependencies: %s", ",".join(excluded_anaconda_deps))
            snowflake_requirements = [
                r
                for r in snowflake_requirements
                if r.name not in excluded_anaconda_deps
            ]
        # remove snowflake-connector-python
        requirement_yaml_lines = [
            # unsure if streamlit supports versioned requirements,
            # following PrPr docs convention for now
            f"- {req.name}"
            for req in snowflake_requirements
            if req.name != "snowflake-connector-python"
        ]
        dependencies_list = "\n".join(requirement_yaml_lines)
        environment = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
        template = environment.get_template("environment.yml.jinja")
        with open("environment.yml", "w", encoding="utf-8") as f:
            f.write(template.render(dependencies=dependencies_list))
        return Path("environment.yml")
    return None


def generate_streamlit_package_wrapper(
    stage_name: str, main_module: str, extract_zip: bool
) -> Path:
    """Uses a jinja template to generate a streamlit wrapper.
    The wrapper will add app.zip to the path and import the app module.
    """
    environment = Environment(loader=FileSystemLoader(TEMPLATES_PATH))
    template = environment.get_template("streamlit_app_launcher.py.jinja")
    target_file = Path("streamlit_app_launcher.py")
    content = template.render(
        {
            "stage_name": stage_name,
            "main_module": main_module,
            "extract_zip": extract_zip,
        }
    )
    with open(target_file, "w", encoding="utf-8") as output_file:
        output_file.write(content)
    return target_file
