from __future__ import annotations

import os
import uuid

import pytest
from snowflake import connector
from snowflake.cli.api.exceptions import EnvironmentVariableNotFoundError
from contextlib import contextmanager
from unittest import mock

_ENV_PARAMETER_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION"
SCHEMA_ENV_PARAMETER = f"{_ENV_PARAMETER_PREFIX}_SCHEMA"
DATABASE_ENV_PARAMETER = f"{_ENV_PARAMETER_PREFIX}_DATABASE"


def add_uuid_to_name(name: str) -> str:
    return f"{name}_{uuid.uuid4().hex}"


@contextmanager
def mock_single_env_var(name: str, value: str):
    env = dict(os.environ)
    env[name] = value
    with mock.patch.dict(os.environ, env):
        yield


def _escape_name(name: str) -> str:
    if "-" in name:
        name = f'"{name}"'
    return name


@contextmanager
def setup_test_database(snowflake_session, database_name: str):
    database_name = _escape_name(database_name)
    snowflake_session.execute_string(
        f"create database {database_name}; use database {database_name}; use schema public;"
    )
    with mock_single_env_var(DATABASE_ENV_PARAMETER, value=database_name):
        yield
    snowflake_session.execute_string(f"drop database {database_name}")


@contextmanager
def setup_test_schema(snowflake_session, schema_name: str):
    schema_name = _escape_name(schema_name)
    snowflake_session.execute_string(
        f"create schema {schema_name}; use schema {schema_name};"
    )
    with mock_single_env_var(SCHEMA_ENV_PARAMETER, value=schema_name):
        yield
    snowflake_session.execute_string(f"drop schema {schema_name}")


@pytest.fixture(scope="function")
def test_database(snowflake_session):
    database_name = add_uuid_to_name("db")
    with setup_test_database(snowflake_session, database_name):
        yield database_name


@pytest.fixture(scope="function")
def test_role(snowflake_session):
    role_name = f"role_{uuid.uuid4().hex}"
    snowflake_session.execute_string(
        f"create role {role_name}; grant role {role_name} to user {snowflake_session.user};"
    )
    yield role_name
    snowflake_session.execute_string(f"drop role {role_name}")


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
