from __future__ import annotations

import logging
from pathlib import Path
from typing import Tuple

import click
import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.commands.decorators import (
    with_experimental_behaviour,
    with_project_definition,
)
from snowflake.cli.api.commands.flags import ReplaceOption, like_option
from snowflake.cli.api.commands.project_initialisation import add_init_command
from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.output.types import (
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.plugins.object.commands import (
    ScopeOption,
)
from snowflake.cli.plugins.object.commands import (
    describe as object_describe,
)
from snowflake.cli.plugins.object.commands import (
    drop as object_drop,
)
from snowflake.cli.plugins.object.commands import (
    list_ as object_list,
)
from snowflake.cli.plugins.streamlit.manager import StreamlitManager

app = SnowTyper(
    name="streamlit",
    help="Manages a Streamlit app in Snowflake.",
)
log = logging.getLogger(__name__)


StreamlitNameArgument = typer.Argument(
    ..., help="Name of the Streamlit app.", show_default=False
)
StageNameOption: str = typer.Option(
    "streamlit",
    "--stage",
    help="Name of the stage where you want to upload Streamlit app files.",
)
OpenOption = typer.Option(
    False,
    "--open",
    help="Whether to open the Streamlit app in a browser.",
    is_flag=True,
)
StreamlitLikeOption = like_option(
    help_example='`list --like "my%"` lists all streamlit apps that begin with “my”',
)
STREAMLIT_OBJECT_TYPE = ObjectType.STREAMLIT.value.cli_name

add_init_command(
    app,
    project_type="Streamlit",
    template="default_streamlit",
    help_message="Name of the Streamlit app project directory you want to create. Defaults to `example_streamlit`.",
)


@app.command("share", requires_connection=True)
def streamlit_share(
    name: str = StreamlitNameArgument,
    to_role: str = typer.Argument(
        ..., help="Role with which to share the Streamlit app."
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


@app.command("deploy", requires_connection=True)
@with_project_definition("streamlit")
@with_experimental_behaviour()
def streamlit_deploy(
    replace: bool = ReplaceOption(
        help="Replace the Streamlit app if it already exists."
    ),
    open_: bool = OpenOption,
    **options,
) -> CommandResult:
    """
    Deploys a Streamlit app defined in the project definition file (snowflake.yml). By default, the command uploads
    environment.yml and any other pages or folders, if present. If you don’t specify a stage name, the `streamlit`
    stage is used. If the specified stage does not exist, the command creates it.
    """
    streamlit: Streamlit = cli_context.project_definition
    if not streamlit:
        return MessageResult("No streamlit were specified in project definition.")

    environment_file = streamlit.env_file
    if environment_file and not Path(environment_file).exists():
        raise ClickException(f"Provided file {environment_file} does not exist")
    elif environment_file is None:
        environment_file = "environment.yml"

    pages_dir = streamlit.pages_dir
    if pages_dir and not Path(pages_dir).exists():
        raise ClickException(f"Provided file {pages_dir} does not exist")
    elif pages_dir is None:
        pages_dir = "pages"

    url = StreamlitManager().deploy(
        streamlit_name=streamlit.name,
        environment_file=Path(environment_file),
        pages_dir=Path(pages_dir),
        stage_name=streamlit.stage,
        main_file=Path(streamlit.main_file),
        replace=replace,
        query_warehouse=streamlit.query_warehouse,
        additional_source_files=streamlit.additional_source_files,
        **options,
    )

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")


@app.command("get-url", requires_connection=True)
def get_url(
    name: str = StreamlitNameArgument,
    open_: bool = OpenOption,
    **options,
):
    """Returns a URL to the specified Streamlit app"""
    url = StreamlitManager().get_url(streamlit_name=name)
    if open_:
        typer.launch(url)
    return MessageResult(url)


@app.command("list", requires_connection=True)
def list_(
    like: str = StreamlitLikeOption,
    scope: Tuple[str, str] = ScopeOption,
    **options,
):
    """Lists all available streamlit apps."""
    object_list(object_type=STREAMLIT_OBJECT_TYPE, like=like, scope=scope, **options)


@app.command("drop", requires_connection=True)
def drop(streamlit_name: str = StreamlitNameArgument, **options):
    """Drop streamlit app of given name."""
    object_drop(
        object_type=STREAMLIT_OBJECT_TYPE, object_name=streamlit_name, **options
    )


@app.command("describe", requires_connection=True)
def describe(streamlit_name: str = StreamlitNameArgument, **options):
    """Provides description of a streamlit app."""
    object_describe(
        object_type=STREAMLIT_OBJECT_TYPE, object_name=streamlit_name, **options
    )
