#!/usr/bin/env python
# -*- coding: utf-8 -*-

import click
from distutils.dir_util import copy_tree
import os
from pathlib import Path
import pkg_resources
import tempfile
import re
from rich import print
from rich.console import Console
from rich.table import Table
import typer

from snowcli import utils, config
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig
from snowcli.utils import print_db_cursor, print_list_tuples

console = Console()
app = typer.Typer()
EnvironmentOption = typer.Option("dev", help='Environment name')

@app.command("init")
def function_init():
    """
    Initialize this directory with a sample set of files to create a function.
    """
    copy_tree(pkg_resources.resource_filename(
        'templates', 'default_function'), f'{os.getcwd()}')

@app.command("create")
def function_create(environment: str = EnvironmentOption,
                    name: str = typer.Option(..., '--name', '-n', help="Name of the function"),
                    file: Path = typer.Option('app.py',
                                              '--file',
                                              '-f', 
                                              help='Path to the file or folder to deploy',
                                              exists=True,
                                              readable=True,
                                              file_okay=True),
                    handler: str = typer.Option(...,
                                                '--handler',
                                                '-h',
                                                help='Handler'),
                    input_parameters: str = typer.Option(...,
                                                         '--input-parameters',
                                                         '-i',
                                                         help='Input parameters'),
                    return_type: str = typer.Option(...,
                                                    '--return-type',
                                                    '-r',
                                                    help='Return type'),
                    overwrite: bool = typer.Option(False,
                                                   '--overwrite',
                                                   '-o',
                                                   help='Overwrite / replace if existing function')
                    ):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()

    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(env_conf['database'], env_conf['schema'], name)
        click.echo('Uploading deployment file to stage...')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=overwrite, role=env_conf['role'])
        packages = utils.getSnowflakePackages()
        click.echo('Creating function...')
        click.echo(
            config.snowflake_connection.createFunction(name=name, inputParameters=input_parameters,
                                                       returnType=return_type,
                                                       handler=handler,
                                                       imports=deploy_dict['full_path'],
                                                       database=env_conf['database'],
                                                       schema=env_conf['schema'],
                                                       role=env_conf['role'],
                                                       warehouse=env_conf['warehouse'],
                                                       overwrite=overwrite,
                                                       packages=packages
                                                       )
        )


@app.command("update")
def function_update(environment: str = EnvironmentOption,
                    name: str = typer.Option(..., '--name', '-n', help="Name of the function"),
                    file: Path = typer.Option('app.py',
                                              '--file',
                                              '-f', 
                                              help='Path to the file to update',
                                              exists=True,
                                              readable=True,
                                              file_okay=True)):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(env_conf['database'], env_conf['schema'], name)
        click.echo(f'Finding function {name}...')
        functions = config.snowflake_connection.listFunctions(
            database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'], like=name)
        if len(functions) == 0:
            click.echo(f'No functions named: {name} found')
            return
        else:
            # if functions == 1, get function arguments property and regular expression till the fist )
            # if function > 1, get all the function arguments properties and regular expression till the first ) and then ask the user to select which one
            function_signature = None
            # regular expression to return "HELLOFUNCTION()" from "HELLOFUNCTION() RETURN VARCHAR"
            regex = re.compile(r'(^.*\(.*\))')
            if len(functions) == 1:
                function = functions[0]

                # get the first group from regex
                function_signature = regex.search(
                    function[8]).group(1)
                click.echo(f'Found function {function_signature}')
            else:
                click.echo(f'Found {len(functions)} like: {name}')
                function_signatures = []
                for function in functions:
                    function_signatures.append(regex.search(
                        function[8]).group(1))
                function_signature = click.prompt(
                    'Please select the function you want to deploy', type=click.Choice(function_signatures))
        click.echo(f'Deploying new file for {name}...')
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            deploy_response = config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=True, role=env_conf['role'])
        click.echo(
            f'{deploy_response} uploaded to stage {deploy_dict["full_path"]}')
        click.echo(f'Checking if any new packages to update...')
        function_details = config.snowflake_connection.describeFunction(
            function=function_signature, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
        function_json = utils.convertFunctionDetailsToDict(function_details)
        anaconda_packages = function_json['packages']
        click.echo(
            f'Found {len(anaconda_packages)} defined Anaconda packages in deployed function...')
        click.echo(
            f'Checking if any packages defined or missing from requirements.snowflake.txt...')
        updatedPackageList = utils.getSnowflakePackagesDelta(anaconda_packages)
        if updatedPackageList:
            click.echo(f'Replacing function with updated packages...')
            config.snowflake_connection.createFunction(
                name=name,
                inputParameters=function_json['signature'].strip('()'),
                returnType=function_json['returns'],
                handler=function_json['handler'],
                imports=deploy_dict['full_path'],
                database=env_conf['database'],
                schema=env_conf['schema'],
                role=env_conf['role'],
                warehouse=env_conf['warehouse'],
                overwrite=True,
                packages=updatedPackageList)
            click.echo(
                f'Function {name} updated with new packages. Deployment complete!')
        else:
            click.echo(f'No packages to update. Deployment complete!')

@app.command("package")
def function_package():
    click.echo('Resolving any requirements from requirements.txt...')
    requirements = utils.parseRequirements()
    pack_dir: str = None
    if requirements:
        click.echo('Comparing provided packages from Snowflake Anaconda...')
        parsedRequirements = utils.parseAnacondaPackages(requirements)
        if not parsedRequirements['other']:
            click.echo('No packages to manually resolve')
        if parsedRequirements['other']:
            click.echo('Writing requirements.other.txt...')
            with open('requirements.other.txt', 'w') as f:
                for package in parsedRequirements['other']:
                    f.write(package + '\n')
        # if requirements.other.txt exists
        if os.path.isfile('requirements.other.txt'):
            if click.confirm('Do you want to try to download non-Anaconda packages?', default=True):
                click.echo('Installing non-Anaconda packages...')
                if utils.installPackages('requirements.other.txt'):
                    pack_dir = 'packages'
        # write requirements.snowflake.txt file
        if parsedRequirements['snowflake']:
            click.echo('Writing requirements.snowflake.txt file...')
            with open('requirements.snowflake.txt', 'w') as f:
                for package in parsedRequirements['snowflake']:
                    f.write(package + '\n')
        if pack_dir:
            utils.recursiveZipPackagesDir(pack_dir, 'app.zip')
        else:
            utils.standardZipDir('app.zip')
    else:
        utils.standardZipDir('app.zip')
    click.echo('\n\nDeployment package now ready: app.zip')

@click.command("list")
def function_list():
    click.echo('Not yet implemented...')


@click.command("delete")
def function_delete():
    click.echo('Not yet implemented...')


@click.command("logs")
def function_logs():
    click.echo('Not yet implemented...')

@app.command("execute")
def function_execute(environment: str = EnvironmentOption,
                     function: str = typer.Option(..., '--function', '-f', help='Function with inputs. E.g. \'hello(int, string)\'')):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.executeFunction(
            function=function, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
        click.echo(results)


@app.command("describe")
def function_describe(environment: str = EnvironmentOption,
                      function: str = typer.Option(..., '--function', '-f', help='Function with inputs. E.g. \'hello(int, string)\'')):
    env_conf = AppConfig().config.get(environment)
    if env_conf is None:
        print("The {environment} environment is not configured in app.toml yet, please run `snow configure dev` first before continuing.")
        raise typer.Abort()
    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.describeFunction(
            function=function, database=env_conf['database'], schema=env_conf['schema'], role=env_conf['role'], warehouse=env_conf['warehouse'])
        print_list_tuples(results)

function = typer.main.get_command(app)
