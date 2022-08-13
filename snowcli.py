import click

@click.command()
def create():
    click.echo('Creating a function...')

@click.command()
def deploy():
    click.echo('Deploying...')

@click.command()
def build():
    click.echo('Building...')

@click.group()
def cli():
    pass

cli.add_command(create)
cli.add_command(build)
cli.add_command(deploy)

def main():
    cli()