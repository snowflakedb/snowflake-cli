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

"""Integration tests for ``snow app setup`` (Snowflake Apps Deploy).

The CLI resolves SnowApps defaults via
``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER`` (see
``SnowflakeAppManager.fetch_snow_apps_parameters``). Source labels in the CLI
call these "account parameters" (``SOURCE_ACCOUNT_PARAM`` in
``apps/commands.py``); fixtures and tests use the same naming.

These parameters are USER-scoped — they cannot be set with ``ALTER SESSION``
and they are shared across every concurrent connection authenticated as the
same user. Because the integration suite runs with
``pytest -n5 --dist=worksteal`` (see ``pyproject.toml``), tests that toggle
these parameters mid-run race each other. We therefore ``SET`` them once
per worker at session start and never ``UNSET`` them; every worker writes
identical values, so concurrent startups are idempotent.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

COMPUTE_POOL = "snowcli_compute_pool"
DATABASE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE", "xsmall")
BUILD_EAI = "cli_test_integration"

_ACCOUNT_PARAMS = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": WAREHOUSE,
    "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": DATABASE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": "public",
}


def _current_user(session) -> str:
    rows = session.execute_string("SELECT CURRENT_USER()")
    return rows[-1].fetchone()[0]


@pytest.fixture(scope="session", autouse=True)
def _ensure_snowflake_apps_account_params(snowflake_session):
    """``SET`` ``DEFAULT_SNOWFLAKE_APPS_*`` USER parameters once per test session.

    Autouse and session-scoped: the fixture runs before any test in this module
    and never tears down. Every xdist worker writes the same values, so
    parallel startups converge on a consistent USER state without locking.

    ``DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION`` is reset to an
    empty string so account-level defaults don't leak into the ``--build-eai``
    resolution path; the empty-string handling is itself part of the contract
    exercised by ``snow app setup``.
    """
    user = _current_user(snowflake_session)
    set_clauses = " ".join(f"{k}='{v}'" for k, v in _ACCOUNT_PARAMS.items())
    snowflake_session.execute_string(f"ALTER USER {user} SET {set_clauses}")
    try:
        snowflake_session.execute_string(
            f"ALTER USER {user} SET DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION=''"
        )
    except Exception:
        pass
    yield


@pytest.mark.integration
def test_setup_creates_valid_yml_with_flags(
    runner,
    temporary_working_directory,
):
    """``snow app setup`` with explicit flags produces a valid snowflake.yml."""
    result = runner.invoke_with_connection(
        [
            "app",
            "setup",
            "--app-name",
            "my_test_app",
            "--compute-pool",
            COMPUTE_POOL,
            "--build-eai",
            BUILD_EAI,
        ]
    )
    assert result.exit_code == 0, result.output
    assert "Initialized Snowflake Apps Deploy project" in result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    assert content["definition_version"] == "2"
    entity = content["entities"]["my_test_app"]
    assert entity["type"] == "snowflake-app"
    assert entity["build_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["service_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["build_eai"]["name"] == BUILD_EAI


@pytest.mark.integration
def test_setup_resolves_from_account_parameters(
    runner,
    temporary_working_directory,
):
    """All ``DEFAULT_SNOWFLAKE_APPS_*`` account parameters drive ``snow app setup``.

    No CLI flags for compute/db/warehouse are passed, so values must come from
    the USER ("account") parameters set by the autouse session fixture.
    Asserts every parameter is reflected in the generated YAML and that the
    source label says ``(account parameter)``.
    """
    result = runner.invoke_with_connection(
        ["app", "setup", "--app-name", "param_app", "--build-eai", BUILD_EAI]
    )
    assert result.exit_code == 0, result.output
    assert "(account parameter)" in result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    entity = content["entities"]["param_app"]
    assert entity["identifier"]["database"] == DATABASE
    assert entity["identifier"]["schema"] == "public"
    assert entity["query_warehouse"] == WAREHOUSE
    assert entity["build_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["service_compute_pool"]["name"] == COMPUTE_POOL


@pytest.mark.integration
def test_setup_dry_run_does_not_write_file(
    runner,
    temporary_working_directory,
):
    """``snow app setup --dry-run`` prints config but does not create snowflake.yml."""
    result = runner.invoke_with_connection(
        [
            "app",
            "setup",
            "--app-name",
            "dry_app",
            "--compute-pool",
            COMPUTE_POOL,
            "--build-eai",
            BUILD_EAI,
            "--dry-run",
        ]
    )
    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert not yml_path.exists(), "snowflake.yml should not be created on dry-run"


@pytest.mark.integration
def test_setup_is_idempotent_when_yml_exists(
    runner,
    temporary_working_directory,
):
    """Running setup when snowflake.yml already exists skips initialization."""
    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    yml_path.write_text("definition_version: '2'\nentities: {}\n")

    result = runner.invoke_with_connection(
        [
            "app",
            "setup",
            "--app-name",
            "dup_app",
            "--compute-pool",
            COMPUTE_POOL,
            "--build-eai",
            BUILD_EAI,
        ]
    )
    assert result.exit_code == 0, result.output
    assert "already exists" in result.output


@pytest.mark.integration
def test_setup_rejects_invalid_app_name(
    runner,
    temporary_working_directory,
):
    """``snow app setup`` rejects app names with invalid characters."""
    result = runner.invoke_with_connection(
        [
            "app",
            "setup",
            "--app-name",
            "bad-name!",
            "--compute-pool",
            COMPUTE_POOL,
            "--build-eai",
            BUILD_EAI,
        ],
        catch_exceptions=True,
    )
    assert result.exit_code != 0
    assert "Invalid app name" in result.output
