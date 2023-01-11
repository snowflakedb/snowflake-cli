from __future__ import annotations

import os
import tempfile
from pathlib import Path

import click
import typer
from rich import print
from snowcli import config
from snowcli import utils
from snowcli.config import AppConfig
from snowcli.utils import generate_deploy_stage_name
from snowcli.utils import print_db_cursor
from snowcli.utils import print_list_tuples
from snowcli.utils import yes_no_ask_callback
from snowcli.utils import YesNoAskOptionsType

# common CLI options
PyPiDownloadOption = typer.Option(
    "ask",
    help="Download non-Anaconda packages from PyPi (yes/no/ask)",
    callback=yes_no_ask_callback,
)
PackageNativeLibrariesOption = typer.Option(
    "ask",
    help="When using packages from PyPi, allow native libraries",
    callback=yes_no_ask_callback,
)
CheckAnacondaForPyPiDependancies: bool = typer.Option(
    True,
    "--check-anaconda-for-pypi-deps",
    "-a",
    help="""When downloading missing Anaconda packages, check if any of
    their dependancies can be imported directly from Anaconda""",
)


def snowpark_create(
    type: str,
    environment: str,
    name: str,
    file: Path,
    handler: str,
    input_parameters: str,
    return_type: str,
    overwrite: bool,
    execute_as_caller: bool = False,
):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"""The {environment} environment is not configured in app.toml
            yet, please run `snow configure {environment}` first before
            continuing.""",
        )
        raise typer.Abort()

    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(
            env_conf["database"],
            env_conf["schema"],
            generate_deploy_stage_name(
                name,
                input_parameters,
            ),
        )
        print("Uploading deployment file to stage...")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path,
                destination_stage=deploy_dict["stage"],
                path=deploy_dict["directory"],
                database=env_conf["database"],
                schema=env_conf["schema"],
                overwrite=overwrite,
                role=env_conf["role"],
            )
        packages = utils.getSnowflakePackages()
        print(f"Creating {type}...")
        if type == "function":
            results = config.snowflake_connection.createFunction(
                name=name,
                inputParameters=input_parameters,
                returnType=return_type,
                handler=handler,
                imports=deploy_dict["full_path"],
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
                overwrite=overwrite,
                packages=packages,
            )
        elif type == "procedure":
            results = config.snowflake_connection.createProcedure(
                name=name,
                inputParameters=input_parameters,
                returnType=return_type,
                handler=handler,
                imports=deploy_dict["full_path"],
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
                overwrite=overwrite,
                packages=packages,
                execute_as_caller=execute_as_caller,
            )
        else:
            raise typer.Abort()
        print_list_tuples(results)


def snowpark_update(
    type: str,
    environment: str,
    name: str,
    file: Path,
    handler: str,
    input_parameters: str,
    return_type: str,
    replace: bool,
    execute_as_caller: bool = False,
) -> None:
    env_conf: dict = AppConfig().config.get(environment)  # type: ignore
    if env_conf is None:
        print(
            f"""The {environment} environment is not configured in app.toml
            yet, please run `snow configure {environment}` first before
            continuing.""",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        updatedPackageList = []
        try:
            print(f"Updating {type} {name}...")
            if type == "function":
                resource_details = config.snowflake_connection.describeFunction(
                    name=name,
                    inputParameters=input_parameters,
                    database=env_conf["database"],
                    schema=env_conf["schema"],
                    role=env_conf["role"],
                    warehouse=env_conf["warehouse"],
                    show_exceptions=False,
                )
            elif type == "procedure":
                resource_details = config.snowflake_connection.describeProcedure(
                    name=name,
                    inputParameters=input_parameters,
                    database=env_conf["database"],
                    schema=env_conf["schema"],
                    role=env_conf["role"],
                    warehouse=env_conf["warehouse"],
                    show_exceptions=False,
                )
            print("Checking if any new packages to update...")
            resource_json = utils.convertResourceDetailsToDict(
                resource_details,
            )  # type: ignore
            anaconda_packages = resource_json["packages"]
            print(
                f"Found {len(anaconda_packages)} defined Anaconda "
                f"packages in deployed {type}...",
            )
            print(
                "Checking if any packages defined or missing from "
                "requirements.snowflake.txt...",
            )
            updatedPackageList = utils.getSnowflakePackagesDelta(
                anaconda_packages,
            )
            print(
                "Checking if app configuration has changed...",
            )
            if (
                resource_json["handler"].lower() != handler.lower()
                or resource_json["returns"].lower() != return_type.lower()
            ):
                print(
                    "Return type or handler types do not match. Replacing "
                    "function configuration...",
                )
                replace = True
        except Exception:
            typer.echo(f"Existing {type} not found, creating new {type}...")
            replace = True

        finally:
            deploy_dict = utils.getDeployNames(
                env_conf["database"],
                env_conf["schema"],
                generate_deploy_stage_name(name, input_parameters),
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
                deploy_response = config.snowflake_connection.uploadFileToStage(
                    file_path=temp_app_zip_path,
                    destination_stage=deploy_dict["stage"],
                    path=deploy_dict["directory"],
                    database=env_conf["database"],
                    schema=env_conf["schema"],
                    overwrite=True,
                    role=env_conf["role"],
                )
            print(
                f"{deploy_response} uploaded to stage " f'{deploy_dict["full_path"]}',
            )

            if updatedPackageList or replace:
                print(f"Replacing {type} with updated values...")
                if type == "function":
                    config.snowflake_connection.createFunction(
                        name=name,
                        inputParameters=input_parameters,
                        returnType=return_type,
                        handler=handler,
                        imports=deploy_dict["full_path"],
                        database=env_conf["database"],
                        schema=env_conf["schema"],
                        role=env_conf["role"],
                        warehouse=env_conf["warehouse"],
                        overwrite=True,
                        packages=utils.getSnowflakePackages(),
                    )
                elif type == "procedure":
                    config.snowflake_connection.createProcedure(
                        name=name,
                        inputParameters=input_parameters,
                        returnType=return_type,
                        handler=handler,
                        imports=deploy_dict["full_path"],
                        database=env_conf["database"],
                        schema=env_conf["schema"],
                        role=env_conf["role"],
                        warehouse=env_conf["warehouse"],
                        overwrite=True,
                        packages=utils.getSnowflakePackages(),
                        execute_as_caller=execute_as_caller,
                    )
                print(
                    f"{type.capitalize()} {name} updated with new packages. "
                    "Deployment complete!",
                )
            else:
                print("No packages to update. Deployment complete!")


def snowpark_package(
    pypi_download: YesNoAskOptionsType,
    check_anaconda_for_pypi_dependencies: bool,
    package_native_libraries: YesNoAskOptionsType,
):
    print("Resolving any requirements from requirements.txt...")
    requirements = utils.parseRequirements()
    pack_dir: str = None  # type: ignore
    if requirements:
        print("Comparing provided packages from Snowflake Anaconda...")
        parsedRequirements = utils.parseAnacondaPackages(requirements)
        if not parsedRequirements["other"]:
            print("No packages to manually resolve")
        if parsedRequirements["other"]:
            print("Writing requirements.other.txt...")
            with open("requirements.other.txt", "w", encoding="utf-8") as f:
                for package in parsedRequirements["other"]:
                    f.write(package + "\n")
        # if requirements.other.txt exists
        if os.path.isfile("requirements.other.txt"):
            do_download = (
                click.confirm(
                    "Do you want to try to download non-Anaconda packages?",
                    default=True,
                )
                if pypi_download == "ask"
                else pypi_download == "yes"
            )
            if do_download:
                print("Installing non-Anaconda packages...")
                should_pack, second_chance_results = utils.installPackages(
                    "requirements.other.txt",
                    check_anaconda_for_pypi_dependencies,
                    package_native_libraries,
                )
                if should_pack:
                    pack_dir = ".packages"
                    # add the Anaconda packages discovered as dependancies
                    if second_chance_results is not None:
                        parsedRequirements["snowflake"] = (
                            parsedRequirements["snowflake"]
                            + second_chance_results["snowflake"]
                        )

        # write requirements.snowflake.txt file
        if parsedRequirements["snowflake"]:
            print("Writing requirements.snowflake.txt file...")
            with open(
                "requirements.snowflake.txt",
                "w",
                encoding="utf-8",
            ) as f:
                for package in parsedRequirements["snowflake"]:
                    f.write(package + "\n")
        if pack_dir:
            utils.recursiveZipPackagesDir(pack_dir, "app.zip")
        else:
            utils.standardZipDir("app.zip")
    else:
        utils.standardZipDir("app.zip")
    print("\n\nDeployment package now ready: app.zip")


def snowpark_execute(type: str, environment: str, select: str):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure dev` first before continuing.",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        if type == "function":
            results = config.snowflake_connection.executeFunction(
                function=select,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        elif type == "procedure":
            results = config.snowflake_connection.executeProcedure(
                procedure=select,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)


def snowpark_describe(
    type: str,
    environment: str,
    name: str,
    input_parameters: str,
    signature: str,
):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            "The {environment} environment is not configured in app.toml yet, "
            "please run `snow configure dev` first before continuing.",
        )
        raise typer.Abort()

    if config.isAuth():
        config.connectToSnowflake()
        if signature == "":
            if name == "" and input_parameters == "":
                typer.BadParameter(
                    "Please provide either a function name and input "
                    "parameters or a function signature",
                )
            signature = (
                name
                + config.snowflake_connection.generate_signature_from_params(
                    input_parameters,
                )
            )
        if type == "function":
            results = config.snowflake_connection.describeFunction(
                signature=signature,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        elif type == "procedure":
            results = config.snowflake_connection.describeProcedure(
                signature=signature,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        else:
            raise typer.Abort()
        print_list_tuples(results)


def snowpark_list(type, environment, like):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            f"The {environment} environment is not configured in app.toml "
            f"yet, please run `snow configure dev` first before continuing.",
        )
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        if type == "function":
            results = config.snowflake_connection.listFunctions(
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
                like=like,
            )
        elif type == "procedure":
            results = config.snowflake_connection.listProcedures(
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
                like=like,
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)


def snowpark_drop(
    type: str,
    environment: str,
    name: str,
    input_parameters: str,
    signature: str,
):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print(
            "The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure dev` first before continuing.",
        )
        raise typer.Abort()

    if config.isAuth():
        config.connectToSnowflake()
        if signature == "":
            if name == "" and input_parameters == "":
                typer.BadParameter(
                    "Please provide either a function name and input "
                    "parameters or a function signature",
                )
            signature = (
                name
                + config.snowflake_connection.generate_signature_from_params(
                    input_parameters,
                )
            )
        if type == "function":
            results = config.snowflake_connection.dropFunction(
                signature=signature,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        elif type == "procedure":
            results = config.snowflake_connection.dropProcedure(
                signature=signature,
                database=env_conf["database"],
                schema=env_conf["schema"],
                role=env_conf["role"],
                warehouse=env_conf["warehouse"],
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)
