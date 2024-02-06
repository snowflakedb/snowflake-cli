import logging
from pathlib import Path
from typing import Optional

import click
import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    global_options_with_connection,
    with_experimental_behaviour,
    with_output,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import DEFAULT_CONTEXT_SETTINGS
from snowflake.cli.api.commands.project_initialisation import add_init_command
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.plugins.streamlit.manager import StreamlitManager

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="streamlit",
    help="Manages Streamlit in Snowflake.",
)
log = logging.getLogger(__name__)


StageNameOption: str = typer.Option(
    "streamlit",
    "--stage",
    help="Stage name where Streamlit files will be uploaded.",
)


add_init_command(app, project_type="Streamlit", template="default_streamlit")


@app.command("share")
@with_output
@global_options_with_connection
def streamlit_share(
    name: str = typer.Argument(..., help="Name of streamlit to share."),
    to_role: str = typer.Argument(
        ..., help="Role that streamlit should be shared with."
    ),
    **options,
) -> CommandResult:
    """
    Shares a Streamlit app with another role.
    """
    cursor = StreamlitManager().share(streamlit_name=name, to_role=to_role)
    return SingleQueryResult(cursor)


def _default_file_callback(param_name: str):
    from click.core import ParameterSource  # type: ignore

    def _check_file_exists_if_not_default(ctx: click.Context, value):
        if (
            ctx.get_parameter_source(param_name) != ParameterSource.DEFAULT  # type: ignore
            and value
            and not Path(value).exists()
        ):
            raise ClickException(f"Provided file {value} does not exist")
        return Path(value)

    return _check_file_exists_if_not_default


@app.command("deploy")
@with_output
@with_project_definition("streamlit")
@with_experimental_behaviour()
@global_options_with_connection
def streamlit_deploy(
    replace: Optional[bool] = typer.Option(
        False,
        "--replace",
        help="Replace the Streamlit if it already exists.",
        is_flag=True,
    ),
    open_: bool = typer.Option(
        False, "--open", help="Whether to open Streamlit in a browser.", is_flag=True
    ),
    **options,
) -> CommandResult:
    """
    Uploads local files to specified stage and creates a Streamlit dashboard using the files. You must specify the
    main python file and query warehouse. By default, the command will upload environment.yml and pages/ folder
    if present. If you don't provide any stage name then 'streamlit' stage will be used. If provided, a stage will be
    created if it does not exist.
    You can modify the behaviour using flags. For details check help information.
    """
    streamlit = cli_context.project_definition
    if not streamlit:
        return MessageResult("No streamlit were specified in project definition.")

    environment_file = streamlit.get("env_file", None)
    if environment_file and not Path(environment_file).exists():
        raise ClickException(f"Provided file {environment_file} does not exist")
    elif environment_file is None:
        environment_file = "environment.yml"

    pages_dir = streamlit.get("pages_dir", None)
    if pages_dir and not Path(pages_dir).exists():
        raise ClickException(f"Provided file {pages_dir} does not exist")
    elif pages_dir is None:
        pages_dir = "pages"

    url = StreamlitManager().deploy(
        streamlit_name=streamlit["name"],
        environment_file=Path(environment_file),
        pages_dir=Path(pages_dir),
        stage_name=streamlit["stage"],
        main_file=Path(streamlit["main_file"]),
        replace=replace,
        query_warehouse=streamlit["query_warehouse"],
        additional_source_files=streamlit.get("additional_source_files"),
        **options,
    )

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")


@app.command("get-url")
@with_output
@global_options_with_connection
def get_url(
    name: str = typer.Argument(..., help="Name of the Streamlit app."),
    open_: bool = typer.Option(
        False, "--open", help="Whether to open Streamlit in a browser.", is_flag=True
    ),
    **options,
):
    """Returns url to provided streamlit app"""
    url = StreamlitManager().get_url(streamlit_name=name)
    if open_:
        typer.launch(url)
    return MessageResult(url)
