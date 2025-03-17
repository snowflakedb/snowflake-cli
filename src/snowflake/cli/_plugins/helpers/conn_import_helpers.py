from __future__ import annotations

import configparser
from ast import literal_eval
from typing import Any, List

import typer
from snowflake.cli.api.config import (
    ConnectionConfig,
    add_connection_to_proper_file,
    get_all_connections,
    set_config_value,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.secure_path import SecurePath


def read_all_connections_from_snowsql(
    default_cli_connection_name: str, snowsql_config_files: List[SecurePath]
) -> dict[str, dict]:
    imported_default_connection: dict[str, Any] = {}
    imported_named_connections: dict[str, dict] = {}

    for file in snowsql_config_files:
        if not file.exists():
            cli_console.step(
                f"SnowSQL config file [{str(file.path)}] does not exist. Skipping."
            )
            continue

        cli_console.step(f"Trying to read connections from [{str(file.path)}].")
        snowsql_config = configparser.ConfigParser()
        snowsql_config.read(file.path)

        if "connections" in snowsql_config and snowsql_config.items("connections"):
            cli_console.step(
                f"Reading SnowSQL's default connection configuration from [{str(file.path)}]"
            )
            snowsql_default_connection = snowsql_config.items("connections")
            imported_default_connection.update(
                _convert_connection_from_snowsql_config_section(
                    snowsql_default_connection
                )
            )

        other_snowsql_connection_section_names = [
            section_name
            for section_name in snowsql_config.sections()
            if section_name.startswith("connections.")
        ]
        for snowsql_connection_section_name in other_snowsql_connection_section_names:
            cli_console.step(
                f"Reading SnowSQL's connection configuration [{snowsql_connection_section_name}] from [{str(file.path)}]"
            )
            snowsql_named_connection = snowsql_config.items(
                snowsql_connection_section_name
            )
            if not snowsql_named_connection:
                cli_console.step(
                    f"Empty connection configuration [{snowsql_connection_section_name}] in [{str(file.path)}]. Skipping."
                )
                continue

            connection_name = snowsql_connection_section_name.removeprefix(
                "connections."
            )
            imported_named_conenction = _convert_connection_from_snowsql_config_section(
                snowsql_named_connection
            )
            if connection_name in imported_named_connections:
                imported_named_connections[connection_name].update(
                    imported_named_conenction
                )
            else:
                imported_named_connections[connection_name] = imported_named_conenction

    def imported_default_connection_as_named_connection():
        name = _validate_imported_default_connection_name(
            default_cli_connection_name, imported_named_connections
        )
        return {name: imported_default_connection}

    named_default_connection = (
        imported_default_connection_as_named_connection()
        if imported_default_connection
        else {}
    )

    return imported_named_connections | named_default_connection


def _validate_imported_default_connection_name(
    name_candidate: str, other_snowsql_connections: dict[str, dict]
) -> str:
    if name_candidate in other_snowsql_connections:
        new_name_candidate = typer.prompt(
            f"Chosen default connection name '{name_candidate}' is already taken by other connection being imported from SnowSQL. Please choose a different name for your default connection"
        )
        return _validate_imported_default_connection_name(
            new_name_candidate, other_snowsql_connections
        )
    else:
        return name_candidate


def _convert_connection_from_snowsql_config_section(
    snowsql_connection: list[tuple[str, Any]],
) -> dict[str, Any]:
    key_names_replacements = {
        "accountname": "account",
        "username": "user",
        "databasename": "database",
        "dbname": "database",
        "schemaname": "schema",
        "warehousename": "warehouse",
        "rolename": "role",
        "private_key_path": "private_key_file",
    }

    def parse_value(value: Any):
        try:
            parsed_value = literal_eval(value)
        except Exception:
            parsed_value = value
        return parsed_value

    cli_connection: dict[str, Any] = {}
    for key, value in snowsql_connection:
        cli_key = key_names_replacements.get(key, key)
        cli_value = parse_value(value)
        cli_connection[cli_key] = cli_value
    return cli_connection


def validate_and_save_connections_imported_from_snowsql(
    default_cli_connection_name: str, all_imported_connections: dict[str, Any]
):
    existing_cli_connection_names: set[str] = set(get_all_connections().keys())
    imported_connections_to_save: dict[str, Any] = {}
    for (
        imported_connection_name,
        imported_connection,
    ) in all_imported_connections.items():
        if imported_connection_name in existing_cli_connection_names:
            override_cli_connection = typer.confirm(
                f"Connection '{imported_connection_name}' already exists in Snowflake CLI, do you want to use SnowSQL definition and override existing connection in Snowflake CLI?"
            )
            if not override_cli_connection:
                continue
        imported_connections_to_save[imported_connection_name] = imported_connection

    for name, connection in imported_connections_to_save.items():
        cli_console.step(f"Saving [{name}] connection in Snowflake CLI's config.")
        add_connection_to_proper_file(name, ConnectionConfig.from_dict(connection))

    if default_cli_connection_name in imported_connections_to_save:
        cli_console.step(
            f"Setting [{default_cli_connection_name}] connection as Snowflake CLI's default connection."
        )
        set_config_value(
            path=["default_connection_name"],
            value=default_cli_connection_name,
        )
