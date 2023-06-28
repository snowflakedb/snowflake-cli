from __future__ import annotations

import logging
import typer
from pathlib import Path
from typing import List, Optional

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor
from snowcli.utils import (
    generate_streamlit_environment_file,
    generate_streamlit_package_wrapper,
)
from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage Streamlit in Snowflake",
)
log = logging.getLogger(__name__)


def get_standard_stage_name(name: str) -> str:
    # Handle embedded stages
    if name.startswith("snow://"):
        return name

    return f"@{name}"


@app.command("list")
def streamlit_list(
    environment: str = ConnectionOption,
    only_cols: List[str] = typer.Option(list, help="Only show these columns"),
):
    """
    List streamlit apps.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.list_streamlits(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )

        print_db_cursor(
            results,
            columns=only_cols,
        )


@app.command("describe")
def streamlit_describe(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Name of streamlit to be deployed."),
):
    """
    Describe a streamlit app.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        description, url = conn.describe_streamlit(
            name,
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
        )
        print_db_cursor(description)
        print_db_cursor(url)


@app.command("create")
def streamlit_create(
    environment: str = ConnectionOption,
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
):
    """
    Create a streamlit app named NAME.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        if from_stage:
            if "." in from_stage:
                full_stage_name = from_stage
            else:
                full_stage_name = f"{conn.ctx.database}.{conn.ctx.schema}.{from_stage}"
            standard_page_name = get_standard_stage_name(full_stage_name)
            from_stage_command = f"FROM {standard_page_name}"
        else:
            from_stage_command = ""

        results = conn.create_streamlit(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            file="streamlit_app_launcher.py" if use_packaging_workaround else file.name,
            from_stage_command=from_stage_command,
        )
        print_db_cursor(results)


@app.command("share")
def streamlit_share(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Name of streamlit to be shared."),
    to_role: str = typer.Argument(
        ..., help="Role that streamlit should be shared with."
    ),
):
    """
    Create a streamlit app named NAME.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.share_streamlit(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
            to_role=to_role,
        )
        print_db_cursor(results)


@app.command("drop")
def streamlit_drop(
    environment: str = ConnectionOption,
    name: str = typer.Argument(..., help="Name of streamlit to be deleted."),
):
    """
    Create a streamlit app named NAME.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        results = conn.drop_streamlit(
            database=conn.ctx.database,
            schema=conn.ctx.schema,
            role=conn.ctx.role,
            warehouse=conn.ctx.warehouse,
            name=name,
        )
        print_db_cursor(results)


@app.command("deploy")
def streamlit_deploy(
    environment: str = ConnectionOption,
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
):
    """
    Deploy streamlit with NAME.
    """
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        schema = conn.ctx.schema
        role = conn.ctx.role
        database = conn.ctx.database
        warehouse = conn.ctx.warehouse
        # THIS WORKAROUND HAS NOT BEEN TESTETD WITH THE NEW STREAMLIT SYNTAX
        if use_packaging_workaround:
            # package an app.zip file, same as the other snowpark_containers_cmds package commands
            snowpark_package(
                pypi_download,  # type: ignore[arg-type]
                check_anaconda_for_pypi_deps,
                package_native_libraries,  # type: ignore[arg-type]
            )
            # upload the resulting app.zip file
            conn.upload_file_to_stage(
                "app.zip",
                f"{name}_stage",
                "/",
                role=role,
                database=database,
                schema=schema,
                warehouse=warehouse,
                overwrite=True,
                create_stage=False,
            )
            main_module = str(file).replace(".py", "")
            file = generate_streamlit_package_wrapper(
                stage_name=f"{name}_stage",
                main_module=main_module,
                extract_zip=packaging_workaround_includes_content,
            )
            # upload the wrapper file
            conn.upload_file_to_stage(
                str(file),
                f"{name}_stage",
                "/",
                role=role,
                database=database,
                schema=schema,
                warehouse=warehouse,
                overwrite=True,
                create_stage=False,
            )
            # if the packaging process generated an environment.snowflake.txt
            # file, convert it into an environment.yml file
            excluded_anaconda_deps_list: Optional[List[str]] = None
            if excluded_anaconda_deps is not None:
                excluded_anaconda_deps_list = excluded_anaconda_deps.split(",")
            env_file = generate_streamlit_environment_file(excluded_anaconda_deps_list)
            if env_file:
                conn.upload_file_to_stage(
                    str(env_file),
                    f"{name}_stage",
                    "/",
                    role=role,
                    database=database,
                    schema=schema,
                    warehouse=warehouse,
                    overwrite=True,
                    create_stage=False,
                )

        base_url = conn.deploy_streamlit(
            name=name,
            file_path=str(file),
            stage_path="/",
            role=role,
            database=database,
            schema=schema,
            warehouse=warehouse,
            overwrite=True,
        )

        def get_url() -> str:
            try:
                host = conn.ctx.host
            except KeyError:
                return base_url

            host_parts = host.split(".")

            if len(host_parts) != 6:
                log.error(
                    f"The connection host ({host}) was missing or not in "
                    "the expected format "
                    "(<account>.<deployment>.snowflakecomputing.com)"
                )
                raise typer.Exit()
            else:
                account_name = host_parts[0]
                deployment = ".".join(host_parts[1:4])

            snowflake_host = conn.ctx.host or "app.snowflake.com"
            uppercased_dsn = f"{database}.{schema}.{name}".upper()
            return (
                f"https://{snowflake_host}/{deployment}/{account_name}/"
                f"#/streamlit-apps/{uppercased_dsn}"
            )

        url = get_url()

        if open_:
            typer.launch(url)
        else:
            log.info(url)
