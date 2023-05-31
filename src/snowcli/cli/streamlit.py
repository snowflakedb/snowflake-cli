#!/usr/bin/env python
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer
from rich import print

from snowcli import config
from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS
from snowcli.config import AppConfig
from snowcli.utils import (
    generate_streamlit_environment_file,
    generate_streamlit_package_wrapper,
)
from snowcli.output.printing import print_db_cursor

app = typer.Typer(
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage Streamlit in Snowflake",
)
EnvironmentOption = typer.Option("dev", help="Environment name")

from snowcli.cli.snowpark_shared import (
    CheckAnacondaForPyPiDependancies,
    PackageNativeLibrariesOption,
    PyPiDownloadOption,
    snowpark_package,
)


def get_standard_stage_name(name: str) -> str:
    # Handle embedded stages
    if name.startswith("snow://"):
        return name

    return f"@{name}"


@app.command("list")
def streamlit_list(
    environment: str = EnvironmentOption,
    only_cols: List[str] = typer.Option(list, help="Only show these columns"),
):
    """
    List streamlit apps.
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.list_streamlits(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
        )

        print_db_cursor(
            results,
            columns=only_cols,
        )


@app.command("describe")
def streamlit_describe(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help="Name of streamlit to be deployed."),
):
    """
    Describe a streamlit app.
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        description, url = config.snowflake_connection.describe_streamlit(
            name,
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
        )
        print_db_cursor(description)
        print_db_cursor(url)


@app.command("create")
def streamlit_create(
    environment: str = EnvironmentOption,
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
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        if from_stage:
            if "." in from_stage:
                full_stage_name = from_stage
            else:
                full_stage_name = (
                    f"{env_conf.get('database')}.{env_conf.get('schema')}.{from_stage}"
                )
            standard_page_name = get_standard_stage_name(full_stage_name)
            from_stage_command = f"FROM {standard_page_name}"
        else:
            from_stage_command = ""

        results = config.snowflake_connection.create_streamlit(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
            file="streamlit_app_launcher.py" if use_packaging_workaround else file.name,
            from_stage_command=from_stage_command,
        )
        print_db_cursor(results)


@app.command("share")
def streamlit_share(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help="Name of streamlit to be shared."),
    to_role: str = typer.Argument(
        ..., help="Role that streamlit should be shared with."
    ),
):
    """
    Create a streamlit app named NAME.
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.share_streamlit(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
            to_role=to_role,
        )
        print_db_cursor(results)


@app.command("drop")
def streamlit_drop(
    environment: str = EnvironmentOption,
    name: str = typer.Argument(..., help="Name of streamlit to be deleted."),
):
    """
    Create a streamlit app named NAME.
    """
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        results = config.snowflake_connection.drop_streamlit(
            database=env_conf.get("database"),
            schema=env_conf.get("schema"),
            role=env_conf.get("role"),
            warehouse=env_conf.get("warehouse"),
            name=name,
        )
        print_db_cursor(results)


@app.command("deploy")
def streamlit_deploy(
    environment: str = EnvironmentOption,
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
    env_conf = AppConfig().config.get(environment)

    if config.is_auth():
        config.connect_to_snowflake()
        schema = env_conf.get("schema")
        role = env_conf.get("role")
        database = env_conf.get("database")
        if use_packaging_workaround:
            # package an app.zip file, same as the other snowpark package commands
            snowpark_package(
                pypi_download,  # type: ignore[arg-type]
                check_anaconda_for_pypi_deps,
                package_native_libraries,  # type: ignore[arg-type]
            )
            # upload the resulting app.zip file
            config.snowflake_connection.upload_file_to_stage(
                "app.zip",
                f"{name}_stage",
                "/",
                role,
                database,
                schema,
                overwrite=True,
            )
            main_module = str(file).replace(".py", "")
            file = generate_streamlit_package_wrapper(
                stage_name=f"{name}_stage",
                main_module=main_module,
                extract_zip=packaging_workaround_includes_content,
            )
            # upload the wrapper file
            config.snowflake_connection.upload_file_to_stage(
                str(file),
                f"{name}_stage",
                "/",
                role,
                database,
                schema,
                overwrite=True,
            )
            # if the packaging process generated an environment.snowflake.txt
            # file, convert it into an environment.yml file
            excluded_anaconda_deps_list: Optional[List[str]] = None
            if excluded_anaconda_deps is not None:
                excluded_anaconda_deps_list = excluded_anaconda_deps.split(",")
            env_file = generate_streamlit_environment_file(excluded_anaconda_deps_list)
            if env_file:
                config.snowflake_connection.upload_file_to_stage(
                    str(env_file),
                    f"{name}_stage",
                    "/",
                    role,
                    database,
                    schema,
                    overwrite=True,
                )

        base_url = config.snowflake_connection.deploy_streamlit(
            name=name,
            file_path=str(file),
            stage_path="/",
            role=role,
            database=database,
            schema=schema,
            overwrite=True,
        )

        def get_url() -> str:
            try:
                host = config.snowflake_connection.connection_config["host"]
            except KeyError:
                return base_url

            host_parts = host.split(".")

            if len(host_parts) != 6:
                print(
                    f"""The connection host ({host}) was missing or not in
                    the expected format
                    (<account>.<deployment>.snowflakecomputing.com)"""
                )
                raise typer.Exit()
            else:
                account_name = host_parts[0]
                deployment = ".".join(host_parts[1:4])

            snowflake_host = env_conf.get(
                "snowflake_host",
                "app.snowflake.com",
            )
            uppercased_dsn = f"{database}.{schema}.{name}".upper()
            return (
                f"https://{snowflake_host}/{deployment}/{account_name}/"
                f"#/streamlit-apps/{uppercased_dsn}"
            )

        url = get_url()

        if open_:
            typer.launch(url)
        else:
            print(url)
