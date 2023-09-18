import logging
import typer
from pathlib import Path
from typing import Optional

from snowcli.cli.common.decorators import global_options_with_connection
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.cli.streamlit.manager import StreamlitManager
from snowcli.output.decorators import with_output
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
)
from snowcli.output.types import (
    CommandResult,
    QueryResult,
    CollectionResult,
    SingleQueryResult,
    MessageResult,
    MultipleResults,
)

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="streamlit",
    help="Manages Streamlit in Snowflake.",
)
log = logging.getLogger(__name__)


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


@app.command("create")
@with_output
@global_options_with_connection
def streamlit_create(
    name: str = typer.Argument(..., help="Name of streamlit to create."),
    file: Path = typer.Option(
        "streamlit_app.py",
        exists=True,
        readable=True,
        file_okay=True,
        help="Path to the Streamlit Python application (`streamlit_app.py`) file.",
    ),
    from_stage: Optional[str] = typer.Option(
        None,
        help="Stage name from which to copy a Streamlit file.",
    ),
    use_packaging_workaround: bool = typer.Option(
        False,
        help="Whether to package all code and dependencies into a zip file. Valid values: `true`, `false` (default). You should use this only for a temporary workaround until native support is available.",
    ),
    **options,
) -> CommandResult:
    """
    Creates a new Streamlit application object in Snowflake. The streamlit is created in database and schema configured in the connection.
    """
    cursor = StreamlitManager().create(
        streamlit_name=name,
        file=file,
        from_stage=from_stage,
        use_packaging_workaround=use_packaging_workaround,
    )
    return SingleQueryResult(cursor)


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


@app.command("deploy")
@with_output
@global_options_with_connection
def streamlit_deploy(
    name: str = typer.Argument(..., help="Name of streamlit to deploy."),
    file: Path = typer.Option(
        "streamlit_app.py",
        exists=True,
        readable=True,
        file_okay=True,
        help="Path of the Streamlit app file.",
    ),
    open_: bool = typer.Option(
        False,
        "--open",
        "-o",
        help="Whether to open Streamlit in a browser. Valid values: `true`, `false`. Default: `false`.",
    ),
    use_packaging_workaround: bool = typer.Option(
        False,
        help="Whether to package all code and dependencies into a zip file. Valid values: `true`, `false` (default). You should use this only for a temporary workaround until native support is available.",
    ),
    packaging_workaround_includes_content: bool = typer.Option(
        False,
        help="Whether to package all code and dependencies into a zip file. Valid values: `true`, `false`. Default: `false`.",
    ),
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    excluded_anaconda_deps: str = typer.Option(
        None,
        help="List of comma-separated package names from `environment.yml` to exclude in the deployed app, particularly when Streamlit fails to import an Anaconda package at runtime. Be aware that excluding files might the risk of runtime errors).",
    ),
    **options,
) -> CommandResult:
    """
    Creates a Streamlit app package for deployment.
    """
    result = StreamlitManager().deploy(
        streamlit_name=name,
        file=file,
        open_in_browser=open_,
        use_packaging_workaround=use_packaging_workaround,
        packaging_workaround_includes_content=packaging_workaround_includes_content,
        pypi_download=pypi_download,
        check_anaconda_for_pypi_deps=check_anaconda_for_pypi_deps,
        package_native_libraries=package_native_libraries,
        excluded_anaconda_deps=excluded_anaconda_deps,
    )
    if result is not None:
        return MessageResult(result)
    return MessageResult("Done")
