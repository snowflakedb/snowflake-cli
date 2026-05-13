# Copyright (c) 2026 Snowflake Inc.
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

"""Shared fixtures for Snowflake Apps Deploy integration tests."""

from __future__ import annotations

import os

import pytest

COMPUTE_POOL = "snowcli_compute_pool"
DATABASE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB")
SCHEMA = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_SCHEMA", "public")
WAREHOUSE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE", "xsmall")
BUILD_EAI = "cli_test_integration"

_ACCOUNT_PARAMS = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": WAREHOUSE,
    "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": DATABASE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": SCHEMA,
    "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION": "",
}


@pytest.fixture(scope="session", autouse=True)
def _ensure_snowflake_apps_account_params(snowflake_session):
    """``SET`` ``DEFAULT_SNOWFLAKE_APPS_*`` USER parameters once per test session.

    Autouse and session-scoped: runs before any test and never tears down.
    Every xdist worker writes the same values, so parallel startups converge on
    a consistent USER state without locking.

    ``DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION`` is set to an
    empty string so any pre-existing account-level value doesn't leak into
    ``--build-eai`` resolution; the empty-string handling is itself part of
    the contract exercised by ``snow app setup``.
    """
    rows = snowflake_session.execute_string("SELECT CURRENT_USER()")
    user = rows[-1].fetchone()[0]
    set_clauses = " ".join(f"{k}='{v}'" for k, v in _ACCOUNT_PARAMS.items())
    snowflake_session.execute_string(f"ALTER USER {user} SET {set_clauses}")
    yield
