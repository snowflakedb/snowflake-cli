# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Configuration testing utilities for testing merged configuration from multiple sources.

This module provides fixtures and utilities for testing configuration resolution
from various sources (SnowSQL config, CLI config, environment variables, CLI params).
"""

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pytest
import tomlkit
from snowflake.cli.api.config_ng import (
    CliArgumentSource,
    ConfigurationResolver,
    EnvironmentSource,
    FileSource,
    IniFileHandler,
    SnowCliEnvHandler,
    SnowSqlEnvHandler,
    TomlFileHandler,
)


@dataclass
class SnowSQLConfig:
    """
    Represents SnowSQL INI-style config file content.

    Args:
        filename: Name of the config file in the configs/ directory
    """

    filename: str


@dataclass
class SnowSQLEnvs:
    """
    Represents SnowSQL environment variables from a file.

    Args:
        filename: Name of the env file in the configs/ directory
    """

    filename: str


@dataclass
class CliConfig:
    """
    Represents CLI TOML config file content.

    Args:
        filename: Name of the config.toml file in the configs/ directory
    """

    filename: str


@dataclass
class CliEnvs:
    """
    Represents CLI environment variables from a file.

    Args:
        filename: Name of the env file in the configs/ directory
    """

    filename: str


@dataclass
class CliParams:
    """
    Represents CLI command-line parameters.

    Args:
        args: Variable length list of CLI arguments (e.g., "--account", "value", "--user", "alice")
    """

    args: Tuple[str, ...]

    def __init__(self, *args: str):
        object.__setattr__(self, "args", args)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert CLI arguments to a dictionary.

        Returns:
            Dictionary with parsed CLI arguments
        """
        result: Dict[str, Any] = {}
        i = 0
        while i < len(self.args):
            if self.args[i].startswith("--"):
                key = self.args[i][2:].replace("-", "_")
                if i + 1 < len(self.args) and not self.args[i + 1].startswith("--"):
                    result[key] = self.args[i + 1]
                    i += 2
                else:
                    result[key] = True
                    i += 1
            else:
                i += 1
        return result


@dataclass
class ConnectionsToml:
    """
    Represents connections.toml file content.

    Args:
        filename: Name of the connections.toml file in the configs/ directory
    """

    filename: str


@dataclass
class FinalConfig:
    """
    Represents the expected final merged configuration.

    Args:
        config_dict: Dictionary of expected configuration values
        connection: Optional connection name to test (default: None for all connections)
        toml_string: Optional TOML string representation for easy reading
    """

    config_dict: Dict[str, Any]
    connection: Optional[str] = None
    toml_string: Optional[str] = None

    def __init__(
        self,
        config_dict: Optional[Dict[str, Any]] = None,
        connection: Optional[str] = None,
        toml_string: Optional[str] = None,
    ):
        """
        Initialize FinalConfig from either a dict or TOML string.
        """
        if toml_string:
            parsed = tomlkit.parse(toml_string)
            object.__setattr__(self, "config_dict", dict(parsed))
        elif config_dict:
            object.__setattr__(self, "config_dict", config_dict)
        else:
            object.__setattr__(self, "config_dict", {})

        object.__setattr__(self, "connection", connection)
        object.__setattr__(self, "toml_string", toml_string)

    def __eq__(self, other):
        """Compare FinalConfig with another FinalConfig or dict."""
        if isinstance(other, FinalConfig):
            return self.config_dict == other.config_dict
        if isinstance(other, dict):
            return self.config_dict == other
        return False

    def __repr__(self):
        """String representation for debugging."""
        if self.toml_string:
            return f"FinalConfig(connection={self.connection}):\n{self.toml_string}"
        return f"FinalConfig({self.config_dict})"


class ConfigSourcesContext:
    """
    Context manager for setting up configuration sources in a temporary environment.

    This class:
    - Creates temporary directories for config files
    - Writes config files from source definitions
    - Sets environment variables
    - Manages cleanup
    """

    def __init__(
        self,
        sources: Tuple[Any, ...],
        configs_dir: Path,
        connection_name: Optional[str] = None,
    ):
        """
        Initialize the config sources context.

        Args:
            sources: Tuple of source definitions (SnowSQLConfig, CliConfig, etc.)
            configs_dir: Path to directory containing config file templates
            connection_name: Optional connection name to resolve
        """
        self.sources = sources
        self.configs_dir = configs_dir
        self.connection_name = connection_name or "a"

        self.temp_dir: Optional[Path] = None
        self.snowsql_dir: Optional[Path] = None
        self.snowflake_dir: Optional[Path] = None
        self.original_env: Dict[str, Optional[str]] = {}
        self.env_vars_to_set: Dict[str, str] = {}
        self.cli_args_dict: Dict[str, Any] = {}

        self.snowsql_config_path: Optional[Path] = None
        self.cli_config_path: Optional[Path] = None
        self.connections_toml_path: Optional[Path] = None

    def __enter__(self):
        """Set up the configuration environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.snowsql_dir = self.temp_dir / ".snowsql"
        self.snowflake_dir = self.temp_dir / ".snowflake"

        self.snowsql_dir.mkdir(exist_ok=True)
        self.snowflake_dir.mkdir(exist_ok=True)

        # Process sources
        for source in self.sources:
            if isinstance(source, SnowSQLConfig):
                self._setup_snowsql_config(source)
            elif isinstance(source, SnowSQLEnvs):
                self._setup_snowsql_envs(source)
            elif isinstance(source, CliConfig):
                self._setup_cli_config(source)
            elif isinstance(source, CliEnvs):
                self._setup_cli_envs(source)
            elif isinstance(source, CliParams):
                self._setup_cli_params(source)
            elif isinstance(source, ConnectionsToml):
                self._setup_connections_toml(source)

        # Set environment variables
        for key, value in self.env_vars_to_set.items():
            self.original_env[key] = os.environ.get(key)
            os.environ[key] = value

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up the configuration environment."""
        # Restore original environment variables
        for key, original_value in self.original_env.items():
            if original_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original_value

        # Clean up temp directory
        if self.temp_dir:
            import shutil

            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _setup_snowsql_config(self, source: SnowSQLConfig):
        """Set up SnowSQL config file."""
        assert self.snowsql_dir is not None
        config_content = (self.configs_dir / source.filename).read_text()
        self.snowsql_config_path = self.snowsql_dir / "config"
        self.snowsql_config_path.write_text(config_content)

    def _setup_snowsql_envs(self, source: SnowSQLEnvs):
        """Set up SnowSQL environment variables from file."""
        env_file = self.configs_dir / source.filename
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                self.env_vars_to_set[key.strip()] = value.strip()

    def _setup_cli_config(self, source: CliConfig):
        """Set up CLI config.toml file."""
        assert self.snowflake_dir is not None
        config_content = (self.configs_dir / source.filename).read_text()
        self.cli_config_path = self.snowflake_dir / "config.toml"
        self.cli_config_path.write_text(config_content)

    def _setup_cli_envs(self, source: CliEnvs):
        """Set up CLI environment variables from file."""
        env_file = self.configs_dir / source.filename
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                self.env_vars_to_set[key.strip()] = value.strip()

    def _setup_cli_params(self, source: CliParams):
        """Set up CLI parameters."""
        self.cli_args_dict = source.to_dict()

    def _setup_connections_toml(self, source: ConnectionsToml):
        """Set up connections.toml file."""
        assert self.snowflake_dir is not None
        config_content = (self.configs_dir / source.filename).read_text()
        self.connections_toml_path = self.snowflake_dir / "connections.toml"
        self.connections_toml_path.write_text(config_content)

    def get_resolver(self) -> ConfigurationResolver:
        """
        Create a ConfigurationResolver with all configured sources.

        Returns:
            ConfigurationResolver instance with all sources configured
        """
        sources_list: List[Any] = []

        # CLI Arguments Source (highest priority)
        if self.cli_args_dict:
            cli_source = CliArgumentSource(cli_context=self.cli_args_dict)
            sources_list.append(cli_source)

        # Environment Variables Source
        env_handlers = [SnowCliEnvHandler(), SnowSqlEnvHandler()]
        env_source = EnvironmentSource(handlers=env_handlers)
        sources_list.append(env_source)

        # File Sources
        file_paths: List[Path] = []
        file_handlers = []

        # Add CLI config files (higher priority)
        if self.cli_config_path and self.cli_config_path.exists():
            file_paths.append(self.cli_config_path)
            file_handlers.append(
                TomlFileHandler(section_path=["connections", self.connection_name])
            )

        if self.connections_toml_path and self.connections_toml_path.exists():
            file_paths.append(self.connections_toml_path)
            file_handlers.append(
                TomlFileHandler(section_path=["connections", self.connection_name])
            )

        # Add SnowSQL config files (lower priority)
        if self.snowsql_config_path and self.snowsql_config_path.exists():
            file_paths.append(self.snowsql_config_path)
            file_handlers.append(
                IniFileHandler(section_path=["connections", self.connection_name])
            )

        if file_paths:
            file_source = FileSource(file_paths=file_paths, handlers=file_handlers)
            sources_list.append(file_source)

        return ConfigurationResolver(sources=sources_list, track_history=True)

    def get_merged_config(self) -> Dict[str, Any]:
        """
        Get the merged configuration from all sources.

        Returns:
            Dictionary with resolved configuration values
        """
        resolver = self.get_resolver()
        return resolver.resolve()


@contextmanager
def config_sources(
    sources: Tuple[Any, ...],
    configs_dir: Optional[Path] = None,
    connection: Optional[str] = None,
):
    """
    Context manager for testing merged configuration from multiple sources.

    Args:
        sources: Tuple of source definitions (SnowSQLConfig, CliConfig, etc.)
        configs_dir: Path to directory containing config file templates (defaults to ./configs/)
        connection: Optional connection name to resolve (defaults to "a")

    Yields:
        ConfigSourcesContext instance for accessing merged configuration

    Example:
        sources = (
            SnowSQLConfig('config'),
            SnowSQLEnvs('snowsql.env'),
            CliConfig('config.toml'),
            CliEnvs('cli.env'),
            CliParams("--account", "test_account", "--user", "alice"),
            ConnectionsToml('connections.toml'),
        )

        with config_sources(sources) as ctx:
            merged = ctx.get_merged_config()
            assert merged["account"] == "test_account"
    """
    if configs_dir is None:
        configs_dir = Path(__file__).parent / "configs"

    context = ConfigSourcesContext(sources, configs_dir, connection)
    with context as ctx:
        yield ctx


@pytest.fixture
def merged_cli_config():
    """
    Fixture that provides a function to get the merged CLI configuration.

    This should be used inside a config_sources context manager.

    Returns:
        Function that returns the merged configuration dictionary
    """

    def _get_merged_config(ctx: ConfigSourcesContext) -> Dict[str, Any]:
        """Get merged configuration from context."""
        return ctx.get_merged_config()

    return _get_merged_config


@pytest.fixture
def make_cli_instance():
    """
    Fixture that provides a function to create a CLI instance.

    Note: This is a placeholder for future implementation if needed.
    For now, we work directly with the resolver.

    Returns:
        Function that creates a CLI instance (placeholder)
    """

    def _make_cli():
        """Create CLI instance placeholder."""
        return None

    return _make_cli
