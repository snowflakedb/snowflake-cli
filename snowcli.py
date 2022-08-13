import click
import configparser

config = configparser.ConfigParser()

@click.command()
def create():
    click.echo('Creating a function...')

@click.command()
def deploy():
    click.echo('Deploying...')

@click.command()
def build():
    click.echo('Building...')

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

def main():
    cli()