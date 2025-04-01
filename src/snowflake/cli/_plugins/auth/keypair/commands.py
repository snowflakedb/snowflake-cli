from pathlib import Path

import typer
from snowflake.cli._plugins.auth.keypair.manager import AuthManager, PublicKeyProperty
from snowflake.cli.api.commands.flags import SecretTypeParser
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import (
    CollectionResult,
    CommandResult,
    MessageResult,
    SingleQueryResult,
)
from snowflake.cli.api.secret import SecretType
from snowflake.cli.api.secure_path import SecurePath

app = SnowTyperFactory(
    name="keypair",
    help="Manages authentication.",
)


KEY_PAIR_DEFAULT_PATH = "~/.ssh"


def _show_connection_name_prompt(ctx: typer.Context, value: str):
    for param in ctx.command.params:
        if param.name == "connection_name":
            if value:
                param.prompt = "Enter connection name"
            break
    return value


_new_connection_option = typer.Option(
    True,
    help="Create a new connection.",
    prompt="Create a new connection?",
    callback=_show_connection_name_prompt,
    show_default=False,
    hidden=True,
)


_connection_name_option = typer.Option(
    None,
    help="The new connection name.",
    prompt=False,
    show_default=False,
    hidden=True,
)


_key_length_option = typer.Option(
    2048,
    "--key-length",
    help="The RSA key length.",
    prompt="Enter key length",
)


_output_path_option = typer.Option(
    KEY_PAIR_DEFAULT_PATH,
    "--output-path",
    help="The output path for private and public keys",
    prompt="Enter output path",
)


_private_key_passphrase_option = typer.Option(
    "",
    "--private-key-passphrase",
    help="The private key passphrase.",
    click_type=SecretTypeParser(),
    prompt="Enter private key passphrase",
    hide_input=True,
    show_default=False,
)


@app.command("setup", requires_connection=True)
def setup(
    new_connection: bool = _new_connection_option,
    connection_name: str = _connection_name_option,
    key_length: int = _key_length_option,
    output_path: Path = _output_path_option,
    private_key_passphrase: SecretType = _private_key_passphrase_option,
    **options,
):
    """
    Generates the key pair, sets the public key for the user in Snowflake, and creates or updates the connection.
    """
    AuthManager().setup(
        connection_name=connection_name,
        key_length=key_length,
        output_path=SecurePath(output_path),
        private_key_passphrase=private_key_passphrase,
    )
    return MessageResult(f"Setup completed.")


@app.command("rotate", requires_connection=True)
def rotate(
    key_length: int = _key_length_option,
    output_path: Path = _output_path_option,
    private_key_passphrase: SecretType = _private_key_passphrase_option,
    **options,
):
    """
    Rotates the key for the connection. Generates the key pair, sets the public key for the user in Snowflake, and creates or updates the connection.
    """
    AuthManager().rotate(
        key_length=key_length,
        output_path=SecurePath(output_path),
        private_key_passphrase=private_key_passphrase,
    )
    return MessageResult(f"Rotate completed.")


@app.command("list", requires_connection=True)
def list_keys(**options) -> CommandResult:
    """
    Lists the public keys set for the user.
    """
    return CollectionResult(AuthManager().list_keys())


@app.command("remove", requires_connection=True)
def remove(
    public_key_property: PublicKeyProperty = typer.Option(
        ...,
        "--key-id",
        help=f"Local path to the template directory or a URL to Git repository with templates.",
        show_default=False,
    ),
    **options,
):
    """
    Removes the public key for the user.
    """
    return SingleQueryResult(
        AuthManager().remove_public_key(public_key_property=public_key_property)
    )


@app.command("status", requires_connection=True)
def status(**options):
    """
    Verifies the key pair configuration and tests the connection.
    """
    AuthManager().status()
    return MessageResult("Status check completed.")
