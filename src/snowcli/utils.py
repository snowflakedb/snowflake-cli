from __future__ import annotations

import glob
import json
import os
import shutil

import click
import requests
import requirements
import typer
from rich import print
from rich.table import Table
from snowcli.config import AppConfig
from snowflake.connector.cursor import SnowflakeCursor


def getDeployNames(database, schema, name) -> dict:
    stage = f'{database}.{schema}.deployments'
    path = f'/{name.lower()}/app.zip'
    directory = f'/{name.lower()}'
    return {
        'stage': stage,
        'path': path,
        'full_path': f'@{stage}{path}',
        'directory': directory,
    }

# create a temporary directory, copy the file_path to it and rename to app.zip


def prepareAppZip(file_path, temp_dir) -> str:
    temp_path = temp_dir + '/app.zip'
    shutil.copy(file_path, temp_path)
    return temp_path


def parseRequirements() -> list[str]:
    reqs = []
    if os.path.exists('requirements.txt'):
        with open('requirements.txt') as f:
            for req in requirements.parse(f):
                reqs.append(req.name)
    else:
        click.echo('No requirements.txt found')

    return reqs

# parse JSON from https://repo.anaconda.com/pkgs/snowflake/channeldata.json and
# return a list of packages that exist in packages with the .packages json
# response from https://repo.anaconda.com/pkgs/snowflake/channeldata.json
# CURRENTLY DOES NOT SUPPORT PINNING TO VERSIONS


def parseAnacondaPackages(packages: list[str]) -> dict:
    url = 'https://repo.anaconda.com/pkgs/snowflake/channeldata.json'
    response = requests.get(url)
    snowflakePackages = []
    otherPackages = []
    if response.status_code == 200:
        channel_data = response.json()
        for package in packages:
            if package in channel_data['packages']:
                snowflakePackages.append(
                    f'{package}',
                )
            else:
                click.echo(
                    f'"{package}" not found in Snowflake anaconda channel...',
                )
                otherPackages.append(package)
        return {'snowflake': snowflakePackages, 'other': otherPackages}
    else:
        click.echo(f'Error: {response.status_code}')
        return {}


def installPackages(file_name: str) -> bool:
    os.system(f'pip install -t .packages/ -r {file_name}')
    click.echo('Checking to see if packages have native libaries...\n')
    # use glob to see if any files in packages have a .so extension
    if glob.glob('.packages/**/*.so'):
        for path in glob.glob('.packages/**/*.so'):
            click.echo(f'Potential native library: {path}')
        if click.confirm(
            '\n\nWARNING! Some packages appear to have native libraries!\n'
            'Continue with package installation?',
            default=False,
        ):
            return True
        else:
            shutil.rmtree('.packages')
            return False
    else:
        click.echo('No native libraries found in packages (Good news!)...')
        return True


def recursiveZipPackagesDir(pack_dir: str, dest_zip: str) -> bool:
    prevdir = os.getcwd()
    os.chdir(f'./{pack_dir}')
    os.system(f'zip -r ../{dest_zip} .')
    os.chdir(prevdir)
    os.system(f'zip -r -g {dest_zip} . -x ".*" -x "{pack_dir}/*"')
    return True


def standardZipDir(dest_zip: str) -> bool:
    os.system(f'zip -r {dest_zip} . -x ".*"')
    return True


def getSnowflakePackages() -> list[str]:
    if os.path.exists('requirements.snowflake.txt'):
        with open('requirements.snowflake.txt') as f:
            return [line.strip() for line in f]
    else:
        return []


def getSnowflakePackagesDelta(anaconda_packages) -> list[str]:
    updatedPackageList = []
    if os.path.exists('requirements.snowflake.txt'):
        with open('requirements.snowflake.txt') as f:
            # for each line, check if it exists in anaconda_packages. If it
            # doesn't, add it to the return string
            for line in f:
                if line.strip() not in anaconda_packages:
                    updatedPackageList.append(line.strip())
        return updatedPackageList
    else:
        return updatedPackageList


def convertResourceDetailsToDict(function_details: list[tuple]) -> dict:
    function_dict = {}
    json_properties = ['packages', 'installed_packages']
    for function in function_details:
        if function[0] in json_properties:
            function_dict[function[0]] = json.loads(
                function[1].replace('\'', '"'),
            )
        else:
            function_dict[function[0]] = function[1]
    return function_dict


def print_db_cursor(cursor, only_cols=[]):
    if cursor.description:
        if any(only_cols):
            cols = [
                (index, col[0]) for (index, col) in enumerate(
                    cursor.description,
                ) if col[0] in only_cols
            ]
        else:
            cols = [(index, col[0])
                    for (index, col) in enumerate(cursor.description)]

        table = Table(*[col[1] for col in cols])
        for row in cursor.fetchall():
            filtered_row = [str(row[col_index]) for (col_index, _) in cols]
            try:
                table.add_row(*filtered_row)
            except Exception as e:
                print(type(e))
                print(e.args)
                print(e)
        print(table)


def print_list_tuples(lt: SnowflakeCursor):
    table = Table("Key", "Value")
    for item in lt:
        if (item[0] == "imports"):
            table.add_row(item[0], item[1].strip("[]"))
        else:
            table.add_row(item[0], item[1])
    print(table)


def conf_callback(ctx: typer.Context, param: typer.CallbackParam, value: str):
    if value:
        try:
            app_config = AppConfig().config

            # Initialize the default map
            ctx.default_map = ctx.default_map or {}
            # if app_config has key 'default'
            if 'default' in app_config:
                ctx.default_map.update(
                    app_config.get('default'),
                )  # type: ignore
            if value in app_config:
                # TODO: Merge the config dict into default_map
                # type: ignore
                ctx.default_map.update(app_config.get(value))
        except Exception as ex:
            raise typer.BadParameter(str(ex))
    return value


def generate_deploy_stage_name(name: str, input_parameters: str) -> str:
    return name + \
        input_parameters.replace(
            '(',
            '',
        ).replace(
            ')',
            '',
        ).replace(
            ' ',
            '_',
        ).replace(
            ',',
            '',
        )
