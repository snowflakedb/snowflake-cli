import json
import shutil
import requests
import click
import requirements
import os
import glob


def getDeployNames(database, schema, name) -> dict:
    stage = f'{database}.{schema}.deployments'
    path = f'/{name.lower()}/app.zip'
    directory = f'/{name.lower()}'
    return {
        'stage': stage,
        'path': path,
        'full_path': f'@{stage}{path}',
        'directory': directory
    }

# create a temporary directory, copy the file_path to it and rename to app.zip


def prepareAppZip(file_path, temp_dir) -> str:
    temp_path = temp_dir + '/app.zip'
    shutil.copy(file_path, temp_path)
    return temp_path


def parseRequirements() -> list[str]:
    reqs = []
    if os.path.exists('requirements.txt'):
        with open('requirements.txt', 'r') as f:
            for req in requirements.parse(f):
                reqs.append(req.name)
    else:
        click.echo('No requirements.txt found')

    return reqs

# parse JSON from https://repo.anaconda.com/pkgs/snowflake/channeldata.json and return a list of packages that exist in packages
# with the .packages json response from https://repo.anaconda.com/pkgs/snowflake/channeldata.json
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
                    f'{package}=={channel_data["packages"][package]["version"]}')
            else:
                click.echo(
                    f'"{package}" not found in Snowflake anaconda channel...')
                otherPackages.append(package)
        return {'snowflake': snowflakePackages, 'other': otherPackages}
    else:
        click.echo(f'Error: {response.status_code}')
        return {}


def installPackages(file_name: str) -> bool:
    os.system(f'pip install -t packages/ -r {file_name}')
    click.echo('Checking to see if packages have native libaries...\n')
    # use glob to see if any files in packages have a .so extension
    if glob.glob('packages/*.so'):
        for path in glob.glob('packages/*.so'):
            click.echo(f'Potential native library: {path}')
        if click.confirm('\n\nWARNING! Some packages appear to have native libraries!\nContinue with package installation?', default=False):
            return True
        else:
            shutil.rmtree('packages')
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


def getSnowflakePackages(anaconda_packages) -> list[str]:
    updatedPackageList = []
    if os.path.exists('requirements.snowflake.txt'):
        with open('requirements.snowflake.txt', 'r') as f:
            # for each line, check if it exists in anaconda_packages. If it doesn't, add it to the return string
            for line in f:
                if line.strip() not in anaconda_packages:
                    updatedPackageList.append(line.strip())
        return updatedPackageList
    else:
        return updatedPackageList


def convertPackagesStringToDict(packages: str) -> dict:
    return json.loads(packages)
