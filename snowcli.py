import click
import configparser

config = configparser.ConfigParser()
config.default_section = 'default'
config.read('credentials')

@click.command()
def create():
    if isAuth():
        click.echo('Creating a function...')

@click.command()
def deploy():
    if isAuth():
        click.echo('Deploying...')

@click.command()
def build():
    if isAuth():
        click.echo(f'Building... with {config.get("default", "account")}')

@click.command()
@click.option('--account', prompt=True, help='Snowflake account')
@click.option('--username', prompt=True, help='Snowflake username')
@click.option('--password', prompt=True, hide_input=True, help='Snowflake password')
def login(account, username, password):
    config['default'] = {
        'account': account,
        'username': username,
        'password': password
    }
    with open('credentials', 'w') as configfile:
        config.write(configfile)

@click.group()
def cli():
    pass

cli.add_command(create)
cli.add_command(build)
cli.add_command(deploy)
cli.add_command(login)

def isAuth():
    if not config.has_option('default', 'account'):
        click.echo('You must login first with `snowcli login`')
        return False
    return True

def main():
    cli()