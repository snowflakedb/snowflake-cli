from __future__ import annotations

import os
import pytest
import uuid

from snowflake import connector
from snowcli.exception import EnvironmentVariableNotFoundError

_ENV_PARAMETER_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION_"


@pytest.fixture(scope="session")
def create_database():
    database_name = f"db_{uuid.uuid4().hex}"

    config = {
        "application": "INTEGRATION_TEST",
        "account": _get_from_env("ACCOUNT", True),
        "user": _get_from_env("USER", True),
        "password": _get_from_env("PASSWORD", True),
        "host": _get_from_env("HOST", True),
        "role": _get_from_env("ROLE", True),
    }
    config = {k: v for k, v in config.items() if v is not None}
    connection = connector.connect(**config)

    connection.execute_string(f"create database {database_name}")
    os.environ[f"{_ENV_PARAMETER_PREFIX}DATABASE"] = database_name

    yield database_name

    connection.execute_string(f"drop database {database_name}")
    del os.environ[f"{_ENV_PARAMETER_PREFIX}DATABASE"]


@pytest.fixture(scope="session")
def snowflake_session(create_database):
    config = {
        "application": "INTEGRATION_TEST",
        "account": _get_from_env("ACCOUNT", True),
        "user": _get_from_env("USER", True),
        "password": _get_from_env("PASSWORD", True),
        "host": _get_from_env("HOST", True),
        "database": _get_from_env("DATABASE", True),
        "role": _get_from_env("ROLE", True),
        "schema": _get_from_env("SCHEMA", False),
        "warehouse": _get_from_env("WAREHOUSE", False),
    }
    config = {k: v for k, v in config.items() if v is not None}
    connection = connector.connect(**config)
    yield connection
    connection.close()


def _get_from_env(parameter_name: str, required: bool) -> str | None:
    env_value = os.environ.get(f"{_ENV_PARAMETER_PREFIX}{parameter_name}")
    if required and not env_value:
        raise EnvironmentVariableNotFoundError(
            f"{_ENV_PARAMETER_PREFIX}{parameter_name}"
        ) from None
    return env_value
