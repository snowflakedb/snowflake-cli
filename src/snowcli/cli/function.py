#!/usr/bin/env python
# -*- coding: utf-8 -*-

import click

from distutils.dir_util import copy_tree

from snowcli import utils
from snowcli.config import AppConfig
from snowcli.snowsql_config import SnowsqlConfig

@click.group()
def function():
    pass

@function.command()
def function_init():
    copy_tree(pkg_resources.resource_filename(
        'templates', 'default_function'), f'{os.getcwd()}')

@function.command()
@click.option('--name', '-n', help='Name of the function', required=True)
@click.option('--file', '-f', 'file', type=click.Path(exists=True), required=True, help='Path to the file or folder to deploy')
# @click.option('--imports', help='File imports into the function')
@click.option('--handler', '-h', help='Handler', required=True)
@click.option('--input-parameters', '-i', 'inputParams', help='Input parameters', required=True)
@click.option('--return-type', '-r', 'returnType', help='Return type', required=True)
@click.option('--overwrite', '-o', is_flag=True, help='Overwrite / replace if existing function')
@click.option('--yaml', '-y', help="YAML file with function configuration", callback=utils.readYamlConfig, is_eager=True)
def function_create(name, database, schema, role, warehouse, handler, yaml, inputParams, returnType, overwrite, file):
    print(f'name: {name}')
    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(database, schema, name)
        click.echo('Uploading deployment file to stage...')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(file, temp_dir)
            config.snowflake_connection.uploadFileToStage(
                file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=overwrite, role=role)
        packages = utils.getSnowflakePackages()
        click.echo('Creating function...')
        click.echo(
            config.snowflake_connection.createFunction(name=name, inputParameters=inputParams,
                                                       returnType=returnType,
                                                       handler=handler,
                                                       imports=deploy_dict['full_path'],
                                                       database=database,
                                                       schema=schema,
                                                       role=role,
                                                       warehouse=warehouse,
                                                       overwrite=overwrite,
                                                       packages=packages
                                                       )
        )


@function.command()
@click.option('--name', '-n', help='Name of the function', required=True)
@click.option('--file', '-f', 'file', type=click.Path(exists=True))
@click.option('--yaml', '-y', help="YAML file with function configuration", callback=utils.readYamlConfig, is_eager=True)
def function_update(file, role, database, schema, warehouse, name, yaml):
    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(database, schema, name)
        click.echo(f'Finding function {name}...')
        functions = config.snowflake_connection.listFunctions(
            database=database, schema=schema, role=role, warehouse=warehouse, like=name)
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
                file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=True, role=role)
        click.echo(
            f'{deploy_response} uploaded to stage {deploy_dict["full_path"]}')
        click.echo(f'Checking if any new packages to update...')
        function_details = config.snowflake_connection.describeFunction(
            function=function_signature, database=database, schema=schema, role=role, warehouse=warehouse)
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
                database=database,
                schema=schema,
                role=role,
                warehouse=warehouse,
                overwrite=True,
                packages=updatedPackageList)
            click.echo(
                f'Function {name} updated with new packages. Deployment complete!')
        else:
            click.echo(f'No packages to update. Deployment complete!')

@function.command()
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

@function.command()
def function_list():
    click.echo('Not yet implemented...')


@function.command()
def function_delete():
    click.echo('Not yet implemented...')


@function.command()
def function_logs():
    click.echo('Not yet implemented...')

@function.command()
@click.option('--function', '-f', help='Function with inputs. E.g. \'hello(1, "world")\'', required=True)
@click.option('--yaml', '-y', help="YAML file with function configuration", callback=utils.readYamlConfig, is_eager=True)
def function_execute(database, schema, role, warehouse, yaml, function):
    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.executeFunction(
            function=function, database=database, schema=schema, role=role, warehouse=warehouse)
        click.echo(results)


@function.command()
@click.option('--function', '-f', help='Function with inputs. E.g. \'hello(1, "world")\'', required=True)
@click.option('--yaml', '-y', help="YAML file with function configuration", callback=utils.readYamlConfig, is_eager=True)
def function_describe(database, schema, role, warehouse, yaml, function):
    if config.isAuth():
        config.connectToSnowflake()
        results = config.snowflake_connection.describeFunction(
            function=function, database=database, schema=schema, role=role, warehouse=warehouse)
        click.echo(dump(dict(results), default_flow_style=False))

function.add_command(function_init, 'init')
function.add_command(function_create, 'create')
function.add_command(function_update, 'update')
function.add_command(function_package, 'package')
function.add_command(function_list, 'list')
function.add_command(function_delete, 'delete')
function.add_command(function_logs, 'logs')
function.add_command(function_execute, 'execute')
function.add_command(function_describe, 'describe')
