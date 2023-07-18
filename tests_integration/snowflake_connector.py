from __future__ import annotations

import os
import pytest

from snowflake import connector
from snowcli.exception import EnvironmentVariableNotFoundError

_ENV_PARAMETER_PREFIX = "SNOWFLAKE_CONNECTIONS_INTEGRATION_"


@pytest.fixture(scope="session")
def snowflake_session():
    config = {
        "application": "INTEGRATION_TEST",
        "account": _get_from_env("ACCOUNT", True),
        "user": _get_from_env("USER", True),
        "password": _get_from_env("PASSWORD", True),
        "host": _get_from_env("HOST", True),
        "port": _get_from_env("PORT", True),
        "protocol": _get_from_env("PROTOCOL", True),
        "database": _get_from_env("DATABASE", False),
        "schema": _get_from_env("SCHEMA", False),
        "warehouse": _get_from_env("WAREHOUSE", False),
        "role": _get_from_env("ROLE", False),
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
