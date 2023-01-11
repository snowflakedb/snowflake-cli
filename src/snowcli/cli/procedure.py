#!/usr/bin/env python
from __future__ import annotations

import os
from pathlib import Path
from shutil import copytree

import pkg_resources
import typer
from snowcli.cli.snowpark_shared import CheckAnacondaForPyPiDependancies
from snowcli.cli.snowpark_shared import PackageNativeLibrariesOption
from snowcli.cli.snowpark_shared import PyPiDownloadOption
from snowcli.cli.snowpark_shared import snowpark_create
from snowcli.cli.snowpark_shared import snowpark_describe
from snowcli.cli.snowpark_shared import snowpark_drop
from snowcli.cli.snowpark_shared import snowpark_execute
from snowcli.cli.snowpark_shared import snowpark_list
from snowcli.cli.snowpark_shared import snowpark_package
from snowcli.cli.snowpark_shared import snowpark_update
from snowcli.utils import conf_callback

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
EnvironmentOption = typer.Option(
    "dev",
    help="Environment name",
    callback=conf_callback,
    is_eager=True,
)


@app.command("init")
def procedure_init():
    """
    Initialize this directory with a sample set of files to create a procedure.
    """
    copytree(
        pkg_resources.resource_filename(
            "templates",
            "default_procedure",
        ),
        f"{os.getcwd()}",
        dirs_exist_ok=True,
    )


@app.command("create")
def procedure_create(
    environment: str = EnvironmentOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the procedure",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file or folder to deploy",
        exists=False,
    ),
    handler: str = typer.Option(
        ...,
        "--handler",
        "-h",
        help="Handler",
    ),
    input_parameters: str = typer.Option(
        ...,
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    return_type: str = typer.Option(
        ...,
        "--return-type",
        "-r",
        help="Return type",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Replace if existing procedure",
    ),
    execute_as_caller: bool = typer.Option(
        False,
        "--execute-as-caller",
        help="Execute as caller",
    ),
):
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    snowpark_create(
        "procedure",
        environment,
        name,
        file,
        handler,
        input_parameters,
        return_type,
        overwrite,
        execute_as_caller,
    )


@app.command("update")
def procedure_update(
    environment: str = EnvironmentOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the procedure",
    ),
    file: Path = typer.Option(
        "app.zip",
        "--file",
        "-f",
        help="Path to the file to update",
        exists=False,
    ),
    handler: str = typer.Option(
        ...,
        "--handler",
        "-h",
        help="Handler",
    ),
    input_parameters: str = typer.Option(
        ...,
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    return_type: str = typer.Option(
        ...,
        "--return-type",
        "-r",
        help="Return type",
    ),
    replace: bool = typer.Option(
        False,
        "--replace-always",
        "-r",
        help="Replace procedure, even if no detected changes to metadata",
    ),
    execute_as_caller: bool = typer.Option(
        False,
        "--execute-as-caller",
        help="Execute as caller",
    ),
):
    (pypi_download, check_anaconda_for_pypi_deps, package_native_libraries)
    snowpark_update(
        "procedure",
        environment,
        name,
        file,
        handler,
        input_parameters,
        return_type,
        replace,
        execute_as_caller,
    )


@app.command("package")
def procedure_package(
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
):
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )


@app.command("execute")
def procedure_execute(
    environment: str = EnvironmentOption,
    select: str = typer.Option(
        ...,
        "--procedure",
        "-p",
        help="Procedure with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_execute("procedure", environment, select)


@app.command("describe")
def procedure_describe(
    environment: str = EnvironmentOption,
    name: str = typer.Option("", "--name", "-n", help="Name of the procedure"),
    input_parameters: str = typer.Option(
        "",
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    signature: str = typer.Option(
        "",
        "--procedure",
        "-p",
        help="Procedure signature with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_describe(
        "procedure",
        environment,
        name,
        input_parameters,
        signature,
    )


@app.command("list")
def procedure_list(
    environment: str = EnvironmentOption,
    like: str = typer.Option(
        "%%",
        "--like",
        "-l",
        help='Filter procedures by name - e.g. "hello%"',
    ),
):
    snowpark_list("procedure", environment, like=like)


@app.command("drop")
def procedure_drop(
    environment: str = EnvironmentOption,
    name: str = typer.Option("", "--name", "-n", help="Name of the procedure"),
    input_parameters: str = typer.Option(
        "",
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    signature: str = typer.Option(
        "",
        "--procedure",
        "-p",
        help="Procedure signature with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_drop("procedure", environment, name, input_parameters, signature)
