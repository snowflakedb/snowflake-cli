import click
from click import ClickException


def validate_config(config_path: str) -> None:
    if not config_path.endswith(".toml"):
        raise click.ClickException(
            f"Configuration file must be a TOML file: {config_path}"
        )


def connect(host: str, user: str) -> None:
    if not host:
        raise ClickException("No active connection — check your credentials")


def run_query(cursor, query: str) -> None:
    try:
        cursor.execute(query)
    except Exception as e:
        raise click.ClickException(f"SQL execution failed: {e}")
