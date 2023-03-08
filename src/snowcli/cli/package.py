#!/usr/bin/env python
from __future__ import annotations

import os
import tempfile
from pathlib import Path
from shutil import rmtree

import click
import typer

from snowcli import config, utils
from snowcli.config import AppConfig

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})
EnvironmentOption = typer.Option("dev", help="Environment name")


@app.command("lookup")
def package_lookup(
    name: str = typer.Argument(
        ...,
        help="Name of the package",
    ),
    run_nested: bool = False,
):
    """
    Check to see if a package is available on the Snowflake anaconda channel.
    """
    packageResponse = utils.parseAnacondaPackages([name])
    ## if list has any items

    if len(packageResponse["snowflake"]) > 0:
        print(f"Package {name} is available on the Snowflake anaconda channel.")
        if run_nested:
            print(
                f"No need to create a package. Just include in your `packages` declaration."
            )
    else:
        check_if_native = click.confirm(
            "The package is not in Anaconda. Do you want to try to see if it's supported as a custom package (requires pip)?",
            default=True,
        )
        if check_if_native:
            packages_string = None
            status, results = utils.installPackages(
                perform_anaconda_check=True, package_name=name, file_name=None
            )
            if status and results is not None and len(results["snowflake"]) > 0:
                packages_string = f"The package {name} is supported, but does depend on the following Snowflake supported native libraries you should include the following in your packages: {results['snowflake']}"
            # if .packages subfolder exists, delete it
            if not run_nested and os.path.exists(".packages"):
                rmtree(".packages")
            if packages_string is not None:
                print("\n\n" + packages_string)
            if run_nested and packages_string is not None:
                return packages_string


@app.command("create")
def package_create(
    name: str = typer.Argument(
        ...,
        help="Name of the package",
    )
):
    """
    Create a python package as a zip file that can be uploaded to a stage and imported for a Snowpark python app.
    """
    results_string = package_lookup(name, run_nested=True)
    if os.path.exists(".packages"):
        utils.recursiveZipPackagesDir(".packages", name + ".zip")
        rmtree(".packages")
        print(
            f"\n\nPackage {name}.zip created. You can now upload it to a stage (`snow package upload -f {name}.zip -s packages`) and reference it in your procedure or function."
        )
        if results_string is not None:
            print("\n" + results_string)


@app.command("upload")
def package_upload(
    file: Path = typer.Option(
        ...,
        "--file",
        "-f",
        help="Path to the file to update",
        exists=False,
    ),
    stage: str = typer.Option(
        ...,
        "--stage",
        "-s",
        help="The stage to upload the file to, NOT including @ symbol",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        "-o",
        help="Overwrite the file if it already exists",
    ),
    environment: str = EnvironmentOption,
):
    """
    Upload a python package zip file to a Snowflake stage so it can be referenced in the imports of a procedure or function.
    """
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure` first before continuing.",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            deploy_response = config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path,
                destination_stage=stage,
                path="/",
                database=env_conf["database"],
                schema=env_conf["schema"],
                overwrite=overwrite,
                role=env_conf["role"],
            )
        print(f"Package {file} {deploy_response[6]} to Snowflake @{stage}/{file}.")
        if deploy_response[6] == "SKIPPED":
            print(
                "Package already exists on stage. Consider using --overwrite to overwrite the file."
            )
