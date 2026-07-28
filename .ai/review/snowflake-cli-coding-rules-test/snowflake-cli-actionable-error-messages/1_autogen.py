from snowflake.cli.api.exceptions import CliArgumentError, CliConnectionError, CliError


def load_config(path: str) -> dict:
    import json

    with open(path) as f:
        data = json.load(f)
    if "version" not in data:
        raise CliError(f"Configuration file {path} is missing required field 'version'")
    return data


def get_connection(name: str):
    connections = {}
    if name not in connections:
        raise CliError(f"Connection {name} does not exist")
    return connections[name]


def check_permissions(role: str):
    if role != "admin":
        raise CliError("Unauthorized")


class Deployer:
    def validate(self, config: dict):
        if "account" not in config:
            raise CliArgumentError("Invalid configuration")

    def load_artifact(self, path: str):
        import os

        if not os.path.exists(path):
            raise CliArgumentError(f"File {path} not found")

    def connect(self, host: str):
        if not host.startswith("https://"):
            raise CliConnectionError(f"Cannot connect to {host}")
