from __future__ import annotations

from typing import Optional

from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import TEMPLATES_PATH
from snowflake.cli.api.output.types import CommandResult, MessageResult
from snowflake.cli.api.secure_path import SecurePath
from typer import Argument


def _create_project_template(template_name: str, project_directory: str):
    SecurePath(TEMPLATES_PATH / template_name).copy(
        project_directory, dirs_exist_ok=True
    )


def add_init_command(
    app: SnowTyper, project_type: str, template: str, help_message: Optional[str] = None
):
    @app.command()
    def init(
        project_name: str = Argument(
            f"example_{project_type.lower()}",
            help=(
                help_message
                if help_message is not None
                else f"Name of the {project_type} project you want to create."
            ),
        ),
        **options,
    ) -> CommandResult:
        _create_project_template(template, project_directory=project_name)
        return MessageResult(f"Initialized the new project in {project_name}/")

    project_type_doc = (
        project_type if project_type.lower() != "streamlit" else "Streamlit app"
    )

    init.__doc__ = (
        f"Initializes this directory with a sample set "
        f"of files for creating a {project_type_doc} project."
    )

    return init
