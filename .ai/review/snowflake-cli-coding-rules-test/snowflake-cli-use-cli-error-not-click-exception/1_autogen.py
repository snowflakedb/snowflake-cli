import click
from click import ClickException


def validate_registry_url(url: str) -> str:
    if not url.startswith("https://"):
        raise click.ClickException(f"Image registry URL is malformed: {url}")
    return url


def load_config(path: str) -> dict:
    if not path.endswith(".toml"):
        raise ClickException(f"Configuration file must be a .toml file: {path}")
    return {}
