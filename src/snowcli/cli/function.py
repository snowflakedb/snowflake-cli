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
def function_init():
    """
    Initialize this directory with a sample set of files to create a function.
    """
    copytree(
        pkg_resources.resource_filename(
            "templates",
            "default_function",
        ),
        f"{os.getcwd()}",
        dirs_exist_ok=True,
    )


@app.command("create")
def function_create(
    environment: str = EnvironmentOption,
    pypi_download: str = PyPiDownloadOption,
    package_native_libraries: str = PackageNativeLibrariesOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    name: str = typer.Option(
        ...,
        "--name",
        "-n",
        help="Name of the function",
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
        help="Replace if existing function",
    ),
):
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    snowpark_create(
        type="function",
        environment=environment,
        name=name,
        file=file,
        handler=handler,
        input_parameters=input_parameters,
        return_type=return_type,
        overwrite=overwrite,
    )


@app.command("update")
def function_update(
    environment: str = EnvironmentOption,
    pypi_download: str = PyPiDownloadOption,
    check_anaconda_for_pypi_deps: bool = CheckAnacondaForPyPiDependancies,
    package_native_libraries: str = PackageNativeLibrariesOption,
    name: str = typer.Option(..., "--name", "-n", help="Name of the function"),
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
        help="Replace function, even if no detected changes to metadata",
    ),
):
    snowpark_package(
        pypi_download,  # type: ignore[arg-type]
        check_anaconda_for_pypi_deps,
        package_native_libraries,  # type: ignore[arg-type]
    )
    snowpark_update(
        type="function",
        environment=environment,
        name=name,
        file=file,
        handler=handler,
        input_parameters=input_parameters,
        return_type=return_type,
        replace=replace,
    )


@app.command("package")
def function_package(
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
def function_execute(
    environment: str = EnvironmentOption,
    function: str = typer.Option(
        ...,
        "--function",
        "-f",
        help="Function with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_execute(type="function", environment=environment, select=function)


@app.command("describe")
def function_describe(
    environment: str = EnvironmentOption,
    name: str = typer.Option("", "--name", "-n", help="Name of the function"),
    input_parameters: str = typer.Option(
        "",
        "--input-parameters",
        "-i",
        help="Input parameters - such as (message string, count int)",
    ),
    function: str = typer.Option(
        "",
        "--function",
        "-f",
        help="Function signature with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_describe(
        type="function",
        environment=environment,
        name=name,
        input_parameters=input_parameters,
        signature=function,
    )


@app.command("list")
def function_list(
    environment: str = EnvironmentOption,
    like: str = typer.Option(
        "%%",
        "--like",
        "-l",
        help='Filter functions by name - e.g. "hello%"',
    ),
):
    snowpark_list("function", environment, like=like)


@app.command("drop")
def function_drop(
    environment: str = EnvironmentOption,
    name: str = typer.Option("", "--name", "-n", help="Name of the function"),
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
        help="Function signature with inputs. E.g. 'hello(int, string)'",
    ),
):
    snowpark_drop("function", environment, name, input_parameters, signature)
