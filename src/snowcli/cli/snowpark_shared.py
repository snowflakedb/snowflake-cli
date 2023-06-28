from __future__ import annotations

import os
import tempfile
from pathlib import Path

import click
import logging
import typer

from snowcli import config, utils
from snowcli.config import connect_to_snowflake
from snowcli.output.printing import print_db_cursor
from snowcli.utils import (
    YesNoAskOptionsType,
    generate_deploy_stage_name,
    yes_no_ask_callback,
)

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
    "--check-anaconda-for-pypi-deps/--no-check-anaconda-for-pypi-deps",
    "-a",
    help="""When downloading missing Anaconda packages, check if any of
    their dependancies can be imported directly from Anaconda""",
)
log = logging.getLogger(__name__)


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
    install_coverage_wrapper: bool = False,
):
    if type == "function" and install_coverage_wrapper:
        log.error(
            "You cannot install a code coverage wrapper on a function, only a procedure."
        )
        raise typer.Abort()

    conn = connect_to_snowflake(connection_name=environment)
    if config.is_auth():
        deploy_dict = utils.get_deploy_names(
            conn.ctx.database,
            conn.ctx.schema,
            generate_deploy_stage_name(
                name,
                input_parameters,
            ),
        )
        log.info("Uploading deployment file to stage...")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepare_app_zip(file, temp_dir)
            if install_coverage_wrapper:
                handler = replace_handler_in_zip(
                    proc_name=name,
                    proc_signature=input_parameters,
                    handler=handler,
                    coverage_reports_stage=deploy_dict["stage"],
                    coverage_reports_stage_path=deploy_dict["directory"] + "/coverage",
                    temp_dir=temp_dir,
                    zip_file_path=temp_app_zip_path,
                )
            conn.upload_file_to_stage(
                file_path=temp_app_zip_path,
                destination_stage=deploy_dict["stage"],
                path=deploy_dict["directory"],
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                overwrite=overwrite,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        packages = utils.get_snowflake_packages()
        if install_coverage_wrapper:
            # if we're installing a coverage wrapper, ensure the coverage package included as a dependency
            if "coverage" not in packages:
                packages.append("coverage")
        log.info(f"Creating {type}...")
        if type == "function":
            results = conn.create_function(
                name=name,
                input_parameters=input_parameters,
                return_type=return_type,
                handler=handler,
                imports=deploy_dict["full_path"],
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
                overwrite=overwrite,
                packages=packages,
            )
        elif type == "procedure":
            results = conn.create_procedure(
                name=name,
                input_parameters=input_parameters,
                return_type=return_type,
                handler=handler,
                imports=deploy_dict["full_path"],
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
                overwrite=overwrite,
                packages=packages,
                execute_as_caller=execute_as_caller,
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)


def validate_configuration(env_conf, environment):
    if env_conf is None:
        log.error(
            f"The {environment} environment is not configured in app.toml "
            "yet, please run `snow configure -e {environment}` first before "
            "continuing."
        )
        raise typer.Abort()


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
    install_coverage_wrapper: bool = False,
) -> None:
    conn = connect_to_snowflake(connection_name=environment)
    if type == "function" and install_coverage_wrapper:
        log.error(
            "You cannot install a code coverage wrapper on a function, only a procedure."
        )
        raise typer.Abort()
    if config.is_auth():
        updated_package_list = []
        try:
            log.info(f"Updating {type} {name}...")
            if type == "function":
                resource_details = conn.describe_function(
                    name=name,
                    input_parameters=input_parameters,
                    database=conn.ctx.database,
                    schema=conn.ctx.schema,
                    role=conn.ctx.role,
                    warehouse=conn.ctx.warehouse,
                    show_exceptions=False,
                )
            elif type == "procedure":
                resource_details = conn.describe_procedure(
                    name=name,
                    input_parameters=input_parameters,
                    database=conn.ctx.database,
                    schema=conn.ctx.schema,
                    role=conn.ctx.role,
                    warehouse=conn.ctx.warehouse,
                    show_exceptions=False,
                )
            log.info("Checking if any new packages to update...")
            resource_json = utils.convert_resource_details_to_dict(
                resource_details,
            )  # type: ignore
            anaconda_packages = resource_json["packages"]
            log.info(
                f"Found {len(anaconda_packages)} defined Anaconda "
                "packages in deployed {type}..."
            )
            log.info(
                "Checking if any packages defined or missing from "
                "requirements.snowflake.txt..."
            )
            updated_package_list = utils.get_snowflake_packages_delta(
                anaconda_packages,
            )
            if install_coverage_wrapper:
                # if we're installing a coverage wrapper, ensure the coverage package included as a dependency
                if (
                    "coverage" not in anaconda_packages
                    and "coverage" not in updated_package_list
                ):
                    updated_package_list.append("coverage")
            log.info("Checking if app configuration has changed...")
            if (
                resource_json["handler"].lower() != handler.lower()
                or resource_json["returns"].lower() != return_type.lower()
            ):
                log.info(
                    "Return type or handler types do not match. Replacing"
                    "function configuration..."
                )
                replace = True
        except Exception:
            log.info(f"Existing {type} not found, creating new {type}...")
            replace = True

        finally:
            deploy_dict = utils.get_deploy_names(
                conn.ctx.database,
                conn.ctx.schema,
                generate_deploy_stage_name(name, input_parameters),
            )
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_app_zip_path = utils.prepare_app_zip(file, temp_dir)
                stage_path = deploy_dict["directory"] + "/coverage"
                if install_coverage_wrapper:
                    handler = replace_handler_in_zip(
                        proc_name=name,
                        proc_signature=input_parameters,
                        handler=handler,
                        coverage_reports_stage=deploy_dict["stage"],
                        coverage_reports_stage_path=stage_path,
                        temp_dir=temp_dir,
                        zip_file_path=temp_app_zip_path,
                    )
                deploy_response = conn.upload_file_to_stage(
                    file_path=temp_app_zip_path,
                    destination_stage=deploy_dict["stage"],
                    path=deploy_dict["directory"],
                    database=conn.ctx.database,
                    schema=conn.ctx.schema,
                    overwrite=True,
                    role=conn.ctx.role,
                    warehouse=conn.ctx.warehouse,
                )
            log.info(
                f"{deploy_response[0]} uploaded to stage " f"{deploy_dict['full_path']}"
            )

            if updated_package_list or replace:
                log.info(f"Replacing {type} with updated values...")
                if type == "function":
                    conn.create_function(
                        name=name,
                        input_parameters=input_parameters,
                        return_type=return_type,
                        handler=handler,
                        imports=deploy_dict["full_path"],
                        database=conn.ctx.database,
                        schema=conn.ctx.schema,
                        role=conn.ctx.role,
                        warehouse=conn.ctx.warehouse,
                        overwrite=True,
                        packages=utils.get_snowflake_packages(),
                    )
                elif type == "procedure":
                    conn.create_procedure(
                        name=name,
                        input_parameters=input_parameters,
                        return_type=return_type,
                        handler=handler,
                        imports=deploy_dict["full_path"],
                        database=conn.ctx.database,
                        schema=conn.ctx.schema,
                        role=conn.ctx.role,
                        warehouse=conn.ctx.warehouse,
                        overwrite=True,
                        packages=utils.get_snowflake_packages(),
                        execute_as_caller=execute_as_caller,
                    )
                log.info(
                    f"{type.capitalize()} {name} updated with new packages. "
                    "Deployment complete!"
                )
            else:
                log.info("No packages to update. Deployment complete!")


def replace_handler_in_zip(
    proc_name: str,
    proc_signature: str,
    handler: str,
    temp_dir: str,
    zip_file_path: str,
    coverage_reports_stage: str,
    coverage_reports_stage_path: str,
) -> str:
    """
    Given an existing zipped stored proc artifact, this function inserts a file containing a code coverage
    wrapper, then returns the name of the new handler that the proc should use
    """
    handler_parts = handler.split(".")
    if len(handler_parts) != 2:
        log.error(
            "To install a code coverage wrapper, your handler must be in the format <module>.<function>"
        )
        raise typer.Abort()
    wrapper_file = os.path.join(temp_dir, "snowpark_coverage.py")
    utils.generate_snowpark_coverage_wrapper(
        target_file=wrapper_file,
        proc_name=proc_name,
        proc_signature=proc_signature,
        coverage_reports_stage=coverage_reports_stage,
        coverage_reports_stage_path=coverage_reports_stage_path,
        handler_module=handler_parts[0],
        handler_function=handler_parts[1],
    )
    utils.add_file_to_existing_zip(zip_file=zip_file_path, other_file=wrapper_file)
    return "snowpark_coverage.measure_coverage"


def snowpark_package(
    pypi_download: YesNoAskOptionsType,
    check_anaconda_for_pypi_deps: bool,
    package_native_libraries: YesNoAskOptionsType,
):
    log.info("Resolving any requirements from requirements.txt...")
    requirements = utils.parse_requirements()
    pack_dir: str = None  # type: ignore
    if requirements:
        log.info("Comparing provided packages from Snowflake Anaconda...")
        split_requirements = utils.parse_anaconda_packages(requirements)
        if not split_requirements.other:
            log.info("No packages to manually resolve")
        if split_requirements.other:
            log.info("Writing requirements.other.txt...")
            with open("requirements.other.txt", "w", encoding="utf-8") as f:
                for package in split_requirements.other:
                    f.write(package.line + "\n")
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
                log.info("Installing non-Anaconda packages...")
                should_pack, second_chance_results = utils.install_packages(
                    "requirements.other.txt",
                    check_anaconda_for_pypi_deps,
                    package_native_libraries,
                )
                if should_pack:
                    pack_dir = ".packages"
                    # add the Anaconda packages discovered as dependancies
                    if second_chance_results is not None:
                        split_requirements.snowflake = (
                            split_requirements.snowflake
                            + second_chance_results.snowflake
                        )

        # write requirements.snowflake.txt file
        if split_requirements.snowflake:
            log.info("Writing requirements.snowflake.txt file...")
            with open(
                "requirements.snowflake.txt",
                "w",
                encoding="utf-8",
            ) as f:
                for package in utils.deduplicate_and_sort_reqs(
                    split_requirements.snowflake
                ):
                    f.write(package.line + "\n")
        if pack_dir:
            utils.recursive_zip_packages_dir(pack_dir, "app.zip")
        else:
            utils.standard_zip_dir("app.zip")
    else:
        utils.standard_zip_dir("app.zip")
    log.info("Deployment package now ready: app.zip")


def snowpark_execute(type: str, environment: str, select: str):
    conn = connect_to_snowflake(connection_name=environment)
    if config.is_auth():
        if type == "function":
            results = conn.execute_function(
                function=select,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        elif type == "procedure":
            results = conn.execute_procedure(
                procedure=select,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
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
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        if signature == "":
            if name == "" and input_parameters == "":
                typer.BadParameter(
                    "Please provide either a function name and input "
                    "parameters or a function signature",
                )
            signature = name + conn.generate_signature_from_params(
                input_parameters,
            )
        if type == "function":
            results = conn.describe_function(
                signature=signature,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        elif type == "procedure":
            results = conn.describe_procedure(
                signature=signature,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)


def snowpark_list(type, environment, like):
    conn = connect_to_snowflake(connection_name=environment)
    if config.is_auth():
        if type == "function":
            results = conn.list_functions(
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
                like=like,
            )
        elif type == "procedure":
            results = conn.list_procedures(
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
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
    conn = connect_to_snowflake(connection_name=environment)

    if config.is_auth():
        if signature == "":
            if name == "" and input_parameters == "":
                typer.BadParameter(
                    "Please provide either a function name and input "
                    "parameters or a function signature",
                )
            signature = name + conn.generate_signature_from_params(
                input_parameters,
            )
        if type == "function":
            results = conn.drop_function(
                signature=signature,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        elif type == "procedure":
            results = conn.drop_procedure(
                signature=signature,
                database=conn.ctx.database,
                schema=conn.ctx.schema,
                role=conn.ctx.role,
                warehouse=conn.ctx.warehouse,
            )
        else:
            raise typer.Abort()
        print_db_cursor(results)
