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
from snowcli.output.printing import OutputData

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    name="streamlit",
    help="Manage Streamlit in Snowflake",
)
log = logging.getLogger(__name__)


@app.command("list")
@with_output
@global_options_with_connection
def streamlit_list(**options) -> OutputData:
    """
    List streamlit apps.
    """
    cursor = StreamlitManager().list()
    return OutputData.from_cursor(cursor)


@app.command("describe")
@with_output
@global_options_with_connection
def streamlit_describe(
    name: str = typer.Argument(..., help="Name of streamlit to be deployed."),
    **options,
) -> OutputData:
    """
    Describe a streamlit app.
    """
    description, url = StreamlitManager().describe(streamlit_name=name)
    return OutputData.from_list(
        [OutputData.from_cursor(description), OutputData.from_cursor(url)]
    )


@app.command("create")
@with_output
@global_options_with_connection
def streamlit_create(
    name: str = typer.Argument(..., help="Name of streamlit to be created."),
    file: Path = typer.Option(
        "streamlit_app.py",
        exists=True,
        readable=True,
        file_okay=True,
        help="Path to streamlit file",
    ),
    from_stage: Optional[str] = typer.Option(
        None,
        help="Stage name to copy streamlit file from",
    ),
    use_packaging_workaround: bool = typer.Option(
        False,
        help="Set this flag to package all code and dependencies into a zip file. "
        + "This should be considered a temporary workaround until native support is available.",
    ),
    **options,
) -> OutputData:
    """
    Create a streamlit app.
    """
    cursor = StreamlitManager().create(
        streamlit_name=name,
        file=file,
        from_stage=from_stage,
        use_packaging_workaround=use_packaging_workaround,
    )
    return OutputData.from_cursor(cursor)


@app.command("share")
@with_output
@global_options_with_connection
def streamlit_share(
    name: str = typer.Argument(..., help="Name of streamlit to be shared."),
    to_role: str = typer.Argument(
        ..., help="Role that streamlit should be shared with."
    ),
    **options,
) -> OutputData:
    """
    Share a streamlit app with a role.
    """
    cursor = StreamlitManager().share(streamlit_name=name, to_role=to_role)
    return OutputData.from_cursor(cursor)


@app.command("drop")
@with_output
@global_options_with_connection
def streamlit_drop(
    name: str = typer.Argument(..., help="Name of streamlit to be deleted."),
    **options,
) -> OutputData:
    """
    Drop a streamlit app.
    """
    cursor = StreamlitManager().drop(streamlit_name=name)
    return OutputData.from_cursor(cursor)


@app.command("deploy")
@with_output
@global_options_with_connection
def streamlit_deploy(
    name: str = typer.Argument(..., help="Name of streamlit to be deployed."),
    file: Path = typer.Option(
        "streamlit_app.py",
        exists=True,
        readable=True,
        file_okay=True,
        help="Path to streamlit file",
    ),
    open_: bool = typer.Option(
        False,
        "--open",
        "-o",
        help="Open streamlit in browser.",
    ),
    use_packaging_workaround: bool = typer.Option(
        False,
        help="Set this flag to package all code and dependencies into a zip file. "
        + "This should be considered a temporary workaround until native support is available.",
    ),
    packaging_workaround_includes_content: bool = typer.Option(
        False,
        help="Set this flag to unzip the package to the working directory. "
        + "Use this if your directory contains non-code files that you need "
        + "to access within your Streamlit app.",
    ),
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    excluded_anaconda_deps: str = typer.Option(
        None,
        help="Sometimes Streamlit fails to import an Anaconda package at runtime. "
        + "Provide a comma-separated list of package names to exclude them from "
        + "environment.yml (noting the risk of runtime errors).",
    ),
    **options,
) -> OutputData:
    """
    Deploy a streamlit app.
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
        return OutputData.from_string(result)
    return OutputData.from_string("Done")
