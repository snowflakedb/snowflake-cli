from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import tomlkit
from snowcli.api.exceptions import (
    MissingConfiguration,
    UnsupportedConfigSectionTypeError,
)
from snowflake.connector.config_manager import CONFIG_MANAGER
from snowflake.connector.errors import MissingConfigOptionError
from tomlkit import TOMLDocument, dump
from tomlkit.container import Container
from tomlkit.exceptions import NonExistentKey
from tomlkit.items import Table

log = logging.getLogger(__name__)


class Empty:
    pass


CONFIG_MANAGER.add_option(
    name="snowcli",
    parse_str=tomlkit.parse,
    default=dict(),
)


def config_init(config_file: Path):
    """
    Initializes the app configuration. Config provided via cli flag takes precedence.
    If config file does not exist we create an empty one.
    """
    if not config_file:
        return

    CONFIG_MANAGER.file_path = config_file
    if not config_file.exists():
        _initialise_config(config_file)
    CONFIG_MANAGER.read_config()


def add_connection(name: str, parameters: dict):
    conf_file_cache = CONFIG_MANAGER.conf_file_cache
    if conf_file_cache.get("connections") is None:
        conf_file_cache["connections"] = {}
    conf_file_cache["connections"][name] = parameters
    _dump_config(conf_file_cache)


def connection_exists(connection_name: str) -> bool:
    return config_section_exists("connections", connection_name)


def config_section_exists(*path) -> bool:
    try:
        _find_section(*path)
        return True
    except (KeyError, NonExistentKey, MissingConfigOptionError):
        return False


def get_connection(connection_name: str) -> dict:
    try:
        return get_config_section("connections", connection_name)
    except KeyError:
        raise MissingConfiguration(f"Connection {connection_name} is not configured")


def get_config_section(*path) -> dict:
    section = _find_section(*path)
    if type(section) is Container:
        return {s: _merge_section_with_env(section[s], *path, s) for s in section}
    elif type(section) is Table:
        return _merge_section_with_env(section, *path)
    raise UnsupportedConfigSectionTypeError(type(section))


def get_config_value(*path, key: str, default: Optional[Any] = Empty) -> Any:
    """Looks for given key under nested path in toml file."""
    env_variable = _get_env_value(*path, key=key)
    if env_variable:
        return env_variable
    try:
        return get_config_section(*path)[key]
    except (KeyError, NonExistentKey, MissingConfigOptionError):
        if default is not Empty:
            return default
        raise


def _initialise_config(config_file: Path) -> None:
    os.makedirs(os.path.dirname(config_file), exist_ok=True)
    config_file.touch()
    log.info("Created Snowflake configuration file at %s", CONFIG_MANAGER.file_path)


def _get_env_value(*path, key: str) -> str | None:
    env_variable_name = (
        "SNOWFLAKE_" + "_".join(p.upper() for p in path) + f"_{key.upper()}"
    )
    return os.environ.get(env_variable_name)


def _find_section(*path) -> TOMLDocument:
    section = CONFIG_MANAGER
    idx = 0
    while idx < len(path):
        section = section[path[idx]]
        idx += 1
    return section


def _merge_section_with_env(section: Union[Table, Any], *path) -> Dict[str, str]:
    if isinstance(section, Table):
        env_variables = _get_envs_for_path(*path)
        section_copy = section.copy()
        section_copy.update(env_variables)
        return section_copy.unwrap()
    # It's a atomic value
    return section


def _get_envs_for_path(*path) -> dict:
    env_variables_prefix = "SNOWFLAKE_" + "_".join(p.upper() for p in path)
    return {
        k.replace(f"{env_variables_prefix}_", "").lower(): os.environ[k]
        for k in os.environ.keys()
        if k.startswith(env_variables_prefix)
    }


def _dump_config(conf_file_cache: Dict):
    with open(CONFIG_MANAGER.file_path, "w+") as fh:
        dump(conf_file_cache, fh)
