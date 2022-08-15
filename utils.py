import shutil
import requests
import click
import requirements
import os

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
                snowflakePackages.append(package)
            else:
                click.echo(f'{package} not found in Snowflake anaconda channel.\n Will attempt to install from PyPi.')
                otherPackages.append(package)
        return { 'snowflake': snowflakePackages, 'other': otherPackages }
    else:
        click.echo(f'Error: {response.status_code}')
        return {}
