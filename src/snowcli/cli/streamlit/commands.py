import logging

import click
import typer
from pathlib import Path
from typing import Optional

from click import ClickException
from snowcli.cli.common.decorators import (
    global_options,
    global_options_with_connection,
    with_experimental_behaviour,
)
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.streamlit.manager import StreamlitManager
from snowcli.output.decorators import with_output

from snowcli.output.types import (
    CommandResult,
    QueryResult,
    SingleQueryResult,
    MessageResult,
    MultipleResults,
)
from snowcli.utils import create_project_template

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


@app.command("init")
@with_output
@global_options
def streamlit_init(
    project_name: str = typer.Argument(
        "example_streamlit", help="Name of the Streamlit project you want to create."
    ),
    **options,
) -> CommandResult:
    """
    Initializes this directory with a sample set of files for creating a Streamlit dashboard.
    """
    create_project_template("default_streamlit", project_directory=project_name)
    return MessageResult(f"Initialized the new project in {project_name}/")


@app.command("list")
@with_output
@global_options_with_connection
def streamlit_list(**options) -> CommandResult:
    """
    Lists available Streamlit apps.
    """
    cursor = StreamlitManager().list()
    return QueryResult(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def streamlit_describe(
    name: str = typer.Argument(
        ..., help="Name of the Streamlit app whose description you want to display."
    ),
    **options,
) -> CommandResult:
    """
    Describes the columns in a Streamlit object.
    """
    description, url = StreamlitManager().describe(streamlit_name=name)
    result = MultipleResults()
    result.add(QueryResult(description))
    result.add(SingleQueryResult(url))
    return result


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


@app.command("drop")
@with_output
@global_options_with_connection
def streamlit_drop(
    name: str = typer.Argument(..., help="Name of streamlit to delete."),
    **options,
) -> CommandResult:
    """
    Removes the specified Streamlit object from the current, or specified, schema.
    """
    cursor = StreamlitManager().drop(streamlit_name=name)
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
@with_experimental_behaviour()
@global_options_with_connection
def streamlit_deploy(
    streamlit_name: str = typer.Argument(..., help="Name of Streamlit to deploy."),
    file: Path = typer.Option(
        "app.py",
        exists=True,
        readable=True,
        file_okay=True,
        help="Path of the Streamlit app file.",
    ),
    stage: Optional[str] = StageNameOption,
    environment_file: Path = typer.Option(
        "environment.yml",
        "--env-file",
        help="Environment file to use.",
        file_okay=True,
        dir_okay=False,
        callback=_default_file_callback("environment_file"),
    ),
    pages_dir: Path = typer.Option(
        "pages",
        "--pages-dir",
        help="Directory with Streamlit pages",
        file_okay=False,
        dir_okay=True,
        callback=_default_file_callback("pages_dir"),
    ),
    query_warehouse: Optional[str] = typer.Option(
        ..., "--query-warehouse", help="Query warehouse for this Streamlit."
    ),
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
    if present. If you don't provide any stage name then 'streamlit' stage will be used. If provided stage will be
    created if it does not exist.
    You can modify the behaviour using flags. For details check help information.
    """
    url = StreamlitManager().deploy(
        streamlit_name=streamlit_name,
        environment_file=environment_file,
        pages_dir=pages_dir,
        stage_name=stage,
        main_file=file,
        replace=replace,
        query_warehouse=query_warehouse,
    )

    if open_:
        typer.launch(url)

    return MessageResult(f"Streamlit successfully deployed and available under {url}")
