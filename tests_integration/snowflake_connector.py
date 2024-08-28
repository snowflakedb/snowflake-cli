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

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from typing import Optional
from unittest import mock

import pytest
from snowflake import connector
from snowflake.cli.api.exceptions import EnvironmentVariableNotFoundError
from snowflake.cli._app.snow_connector import update_connection_details_with_private_key

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
        "authenticator": "SNOWFLAKE_JWT",
        "account": _get_from_env("ACCOUNT"),
        "user": _get_from_env("USER"),
        "private_key_file": _get_private_key_file(),
        "host": _get_from_env("HOST", allow_none=True),
        "warehouse": _get_from_env("WAREHOUSE", allow_none=True),
        "role": _get_from_env("ROLE", allow_none=True),
    }
    config = {k: v for k, v in config.items() if v is not None}
    update_connection_details_with_private_key(config)
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


def _get_private_key_file() -> Optional[str]:
    private_key_file = _get_from_env("PRIVATE_KEY_PATH", allow_none=True)
    if private_key_file is not None:
        return private_key_file
    return _get_from_env("PRIVATE_KEY_FILE")
