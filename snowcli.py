from typing_extensions import Required
import click
import config
import click_extensions
import utils
import tempfile

@click.group()
def function():
    pass

def standard_options(function):
    function = click.option('--database', '-d', help='Database name')(function)
    function = click.option('--schema', '-s', help='Schema name')(function)
    function = click.option('--role', '-r', help='Role name')(function)
    function = click.option('--warehouse', '-w', help='Warehouse name')(function)
    return function

@click.command(cls=click_extensions.CommandWithConfigOverload('yaml', config.auth_config))
@standard_options
@click.option('--name', '-n', help='Name of the function', required=True)
@click.option('--file', '-f', 'path', type=click.Path(exists=True), required=True, help='Path to the file or folder to deploy')
# @click.option('--imports', help='File imports into the function')
@click.option('--handler', help='Handler', required=True)
@click.option('--input-parameters', '-i', 'inputParams', help='Input parameters', required=True)
@click.option('--return-type', '-r', 'returnType', help='Return type', required=True)
@click.option('--overwrite', '-o', is_flag=True, help='Overwrite / replace if existing function')
@click.option('--yaml', '-y', help="YAML file with function configuration")
def function_create(name, database, schema, role, warehouse, handler, yaml, inputParams, returnType, overwrite, path):
    if config.isAuth():
        config.connectToSnowflake()
        deploy_dict = utils.getDeployNames(database, schema, name)
        click.echo('Uploading deployment file to stage...')

        # with temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_app_zip_path = utils.prepareAppZip(path, temp_dir)
            config.snowflake_connection.uploadFileToStage(file_path=temp_app_zip_path, destination_stage=deploy_dict['stage'], path=deploy_dict['directory'], overwrite=overwrite, role=role)
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
                overwrite=overwrite
                )
            )

@click.command()
@standard_options
@click.option('--file', '-f', 'file_path', type=click.Path(exists=True))
@click.option('--overwrite', '-o', is_flag=True, help='Overwrite / replace if existing file')
def function_deploy(file_path, overwrite, role):
    if config.isAuth():
        config.connectToSnowflake()
        click.echo('Deploying file...')
        click.echo(config.snowflake_connection.uploadFileToStage(file_path=file_path, destination_stage='~', overwrite=overwrite, role=role))

@click.command()
def function_build():
    if config.isAuth():
        click.echo(f'Building... with {config.auth_config.get("default", "account")}')

@click.command()
@click.option('--account', prompt='Snowflake account', help='Snowflake account')
@click.option('--username', prompt='Snowflake username', help='Snowflake username')
@click.option('--password', prompt='Snowflake password', hide_input=True, help='Snowflake password')
@click.option('--database', prompt='Snowflake database [optional]', default='', help='Snowflake database [optional]', required=False)
@click.option('--schema', prompt='Snowflake schema [optional]', default='', help='Snowflake schema [optional]', required=False)
@click.option('--role', prompt='Snowflake role [optional]', default='', help='Snowflake role [optional]', required=False)
@click.option('--warehouse', prompt='Snowflake warehouse [optional]', default='', help='Snowflake warehouse [optional]', required=False)
def login(account, username, password, database, schema, role, warehouse):
    config.auth_config['default'] = {
        'account': account,
        'username': username,
        'password': password,
        'database': database,
        'schema': schema,
        'role': role,
        'warehouse': warehouse
    }
    with open('credentials', 'w') as configfile:
        config.auth_config.write(configfile)

@click.command()
def function_list():
    click.echo('Not yet implemented...')

@click.command()
def function_delete():
    click.echo('Not yet implemented...')

@click.command()
def function_logs():
    click.echo('Not yet implemented...')

@click.group()
def cli():
    pass

def main():
    cli()

@click.command()
def procedure():
    pass

@click.command()
def streamlit():
    pass

function.add_command(function_create, 'create')
function.add_command(function_deploy, 'deploy')
function.add_command(function_build, 'build')
function.add_command(function_list, 'list')
function.add_command(function_delete, 'delete')
function.add_command(function_logs, 'logs')
cli.add_command(function)
cli.add_command(procedure)
cli.add_command(streamlit)
cli.add_command(login)