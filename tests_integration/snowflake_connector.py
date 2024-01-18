from __future__ import annotations

import os
import uuid

import pytest
from snowflake import connector
from snowflake.cli.api.exceptions import EnvironmentVariableNotFoundError

_ENV_PARAMETER_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION"


@pytest.fixture(scope="function")
def test_database(snowflake_session):
    database_name = f"db_{uuid.uuid4().hex}"
    snowflake_session.execute_string(
        f"create database {database_name}; use database {database_name}; use schema public;"
    )
    os.environ[f"{_ENV_PARAMETER_PREFIX}_DATABASE"] = database_name

    yield database_name

    snowflake_session.execute_string(f"drop database {database_name}")
    del os.environ[f"{_ENV_PARAMETER_PREFIX}_DATABASE"]


@pytest.fixture(scope="session")
def snowflake_session():
    config = {
        "application": "INTEGRATION_TEST",
        "account": _get_from_env("ACCOUNT"),
        "user": _get_from_env("USER"),
        "password": _get_from_env("PASSWORD"),
        "host": _get_from_env("HOST", allow_none=True),
    }
    config = {k: v for k, v in config.items() if v is not None}
    connection = connector.connect(**config)
    yield connection
    connection.close()


def _get_from_env(parameter_name: str, default=None, allow_none=False) -> str | None:
    env_value = os.environ.get(f"{_ENV_PARAMETER_PREFIX}_{parameter_name}")
    if not env_value:
        if default is None and not allow_none:
            raise EnvironmentVariableNotFoundError(
                f"{_ENV_PARAMETER_PREFIX}_{parameter_name}"
            ) from None
        return default
    return env_value
