from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Any

import tomlkit
from tomlkit import dump, table, TOMLDocument
from tomlkit.exceptions import NonExistentKey
import logging

from snowcli.exception import MissingConfiguration
from snowcli.snow_connector import SnowflakeConnector
from snowflake.connector.constants import CONFIG_FILE
from snowflake.connector.config_manager import ConfigManager


log = logging.getLogger(__name__)
snowflake_connection: SnowflakeConnector


class CliConfigManager(ConfigManager):
    def __init__(self, file_path: Path = CONFIG_FILE):
        super().__init__(name="SNOWCLI_PARSER", file_path=file_path)
        self._add_options()

    def from_context(self, config_path_override: Optional[Path]):
        if config_path_override:
            self.file_path = config_path_override
        if not self.file_path.exists():
            self._initialise_config()
        self.read_config()

    def _add_options(self):
        self.add_option(
            name="options",
            parse_str=tomlkit.parse,
        )
        self.add_option(
            name="connections",
            parse_str=tomlkit.parse,
        )

    def get_section(self, *path) -> TOMLDocument:
        section = self
        idx = 0
        while idx < len(path):
            section = section[path[idx]]
            idx += 1
        return section

    def section_exists(self, *path) -> bool:
        try:
            self.get_section(*path)
            return True
        except NonExistentKey:
            return False

    def _get_from_env(self, *path, key: str):
        env_variable_name = (
            "SNOWFLAKE_" + "_".join(p.upper() for p in path) + f"_{key.upper()}"
        )
        return os.environ.get(env_variable_name)

    def get(self, *path, key: str, default: Optional[Any] = None) -> Any:
        """Looks for given key under nested path in toml file."""
        env_variable = self._get_from_env(*path, key=key)
        if env_variable:
            return env_variable
        try:
            return self.get_section(*path)[key]
        except NonExistentKey:
            if default:
                return default
            raise

    def initialize_connection_section(self):
        self.conf_file_cache = TOMLDocument()
        self.conf_file_cache.append("connections", table())

    def _initialise_config(self):
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        self.initialize_connection_section()
        self._dump_config()
        log.info(f"Created Snowflake configuration file at {cli_config.file_path}")

    def get_connection(self, connection_name: str) -> dict:
        try:
            return self.get("connections", key=connection_name)
        except NonExistentKey:
            raise MissingConfiguration(
                f"Connection {connection_name} is not configured"
            )

    def add_connection(self, name: str, parameters: dict):
        if not self.section_exists("connections"):
            self.initialize_connection_section()
        self.get_section("connections").add(name, parameters)
        self._dump_config()

    def _dump_config(self):
        with open(self.file_path, "w+") as fh:
            dump(self.conf_file_cache, fh)


def config_init(config_file: Path):
    """
    Initializes the app configuration. Config provided via cli flag takes precedence.
    If config file does not exist we create an empty one.
    """
    cli_config.from_context(config_path_override=config_file)


def connect_to_snowflake(connection_name: Optional[str] = None, **overrides):  # type: ignore
    connection_name = connection_name if connection_name is not None else "dev"
    return SnowflakeConnector(
        connection_parameters=cli_config.get_connection(connection_name),
        overrides=overrides,
    )


def is_auth():
    # To be removed. Added to simplify refactor
    return True


cli_config: CliConfigManager = CliConfigManager()  # type: ignore
