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

"""Integration tests for ``snow app setup`` (Snowflake App Runtime).

Scope: this module verifies the end-to-end account-parameter resolution path
that *only* a real Snowflake account can exercise. The CLI calls
``SYSTEM$GET_APPLICATION_SERVICE_DEFAULTS()`` (see
``SnowflakeAppManager.fetch_app_service_defaults``), which resolves the
``DEFAULT_SNOWFLAKE_APPS_*`` parameters server-side, and the source label
emitted by the resolver is ``(account parameter)`` (``SOURCE_ACCOUNT_PARAM``
in ``apps/commands.py``); fixtures and tests use the same naming.

CLI surface, validation, dry-run, idempotency, and YAML-generation behavior
are covered by unit tests in ``tests/apps/test_commands.py``; do not duplicate
them here.

These parameters are USER-scoped — they cannot be set with ``ALTER SESSION``
and they are shared across every concurrent connection authenticated as the
same user. Because the integration suite runs with
``pytest -n5 --dist=worksteal`` (see ``pyproject.toml``), tests that toggle
these parameters mid-run race each other. We therefore ``SET`` them once per
worker at session start and never ``UNSET`` them; every worker writes
identical values, so concurrent startups are idempotent.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml
from snowflake.connector.errors import ProgrammingError

DATABASE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB")
WAREHOUSE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE", "xsmall")
BUILD_EAI = "cli_test_integration"
APP_SERVICE_DEFAULTS_FUNCTION = "SYSTEM$GET_APPLICATION_SERVICE_DEFAULTS"

# Destination schema configured as a *quoted*, case-sensitive lower-case
# identifier. Snowflake folds unquoted identifiers to upper case, so a
# lower-case schema round-trips through ``snow app setup`` only if the resolver
# keeps it quoted end-to-end (the system function returns it already
# SQL-quoted) — which is exactly the behavior this exercises. A dedicated name
# (not the shared ``PUBLIC`` schema) guarantees the value is genuinely
# lower-case regardless of the connection's configured schema; it is created in
# ``DATABASE`` by the session fixture below.
SCHEMA = "snowcli_app_lower_schema"  # bare, case-sensitive lower-case name
QUOTED_SCHEMA = f'"{SCHEMA}"'  # how it appears in SQL and in snowflake.yml

_ACCOUNT_PARAMS = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": WAREHOUSE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": DATABASE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": QUOTED_SCHEMA,
    "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION": "",
}


@pytest.fixture(scope="session", autouse=True)
def _ensure_snowflake_apps_account_params(snowflake_session):
    """``SET`` ``DEFAULT_SNOWFLAKE_APPS_*`` USER parameters once per test session.

    Autouse and session-scoped: the fixture runs before any test in this module
    and never tears down. Every xdist worker writes the same values, so
    parallel startups converge on a consistent USER state without locking.

    ``DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION`` is set to an
    empty string so any pre-existing account-level value doesn't leak into
    ``--build-eai`` resolution; the empty-string handling is itself part of
    the contract exercised by ``snow app setup``.

    The quoted lower-case destination schema is created here (idempotent, never
    dropped — same model as the parameters) so the system function resolves to
    it instead of applying its inaccessible-destination fallback.
    """
    rows = snowflake_session.execute_string("SELECT CURRENT_USER()")
    user = rows[-1].fetchone()[0]
    snowflake_session.execute_string(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{QUOTED_SCHEMA}"
    )
    set_clauses = " ".join(f"{k}='{v}'" for k, v in _ACCOUNT_PARAMS.items())
    snowflake_session.execute_string(f"ALTER USER {user} SET {set_clauses}")
    yield


@pytest.fixture(scope="session", autouse=True)
def _require_app_service_defaults_function(snowflake_session):
    """Skip this module when ``SYSTEM$GET_APPLICATION_SERVICE_DEFAULTS()`` is not
    yet available on the test deployment.

    ``snow app setup`` resolves its defaults through this system function (see
    ``SnowflakeAppManager.fetch_app_service_defaults``). The server change that
    adds it rolls out to deployments some time after it merges, so on a
    deployment that has not picked it up yet the call fails with
    ``Unknown function`` and the CLI has nothing to resolve from. Skip rather
    than fail during that rollout window; the test runs normally once the
    function is live.
    """
    try:
        rows = snowflake_session.execute_string(
            f"SELECT {APP_SERVICE_DEFAULTS_FUNCTION}()"
        )
        rows[-1].fetchone()
    except ProgrammingError as exc:
        if "Unknown function" in str(exc) and APP_SERVICE_DEFAULTS_FUNCTION in str(exc):
            pytest.skip(
                f"{APP_SERVICE_DEFAULTS_FUNCTION} is not available on this "
                "deployment yet; skipping until it rolls out."
            )
        raise
    yield


@pytest.mark.integration
def test_setup_resolves_from_account_parameters(
    runner,
    temporary_working_directory,
):
    """All ``DEFAULT_SNOWFLAKE_APPS_*`` account parameters drive ``snow app setup``.

    No CLI flags for db/warehouse are passed, so values must come from
    the USER ("account") parameters set by the autouse session fixture.
    Asserts every resolved field is reflected in the generated YAML and that
    the source label printed by the resolver is ``(account parameter)``.

    The destination schema is configured as a *quoted* lower-case identifier, so
    this also verifies that the system function returns quoted identifiers
    already SQL-quoted and the CLI preserves their case verbatim (rather than
    folding them to upper case). Identifiers are otherwise case-insensitive, so
    the database/warehouse comparisons ignore case.

    Compute pools are intentionally not resolved or written: app services run
    on server-managed compute pools, so ``snow app setup`` never emits
    ``build_compute_pool`` / ``service_compute_pool``.
    """
    result = runner.invoke_with_connection(
        ["app", "setup", "--app-name", "param_app", "--build-eai", BUILD_EAI]
    )
    assert result.exit_code == 0, result.output

    # Database and warehouse are unquoted identifiers; Snowflake folds them to
    # upper case, and they are case-insensitive, so compare ignoring case.
    output_lower = result.output.lower()
    for expected in (
        f"database: {DATABASE}  (account parameter)",
        f"warehouse: {WAREHOUSE}  (account parameter)",
    ):
        assert expected.lower() in output_lower, result.output

    # The schema is a quoted, case-sensitive lower-case identifier: it must be
    # preserved verbatim (quotes kept, not upper-cased). Assert exact case.
    assert (
        f"schema: {QUOTED_SCHEMA}  (account parameter)" in result.output
    ), result.output

    assert "build_compute_pool" not in result.output, result.output
    assert "service_compute_pool" not in result.output, result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    entity = content["entities"]["param_app"]
    assert entity["identifier"]["database"].upper() == DATABASE.upper()
    # Quoted lower-case schema preserved verbatim (quotes kept, case-sensitive).
    assert entity["identifier"]["schema"] == QUOTED_SCHEMA
    assert entity["query_warehouse"].upper() == WAREHOUSE.upper()
    assert "build_compute_pool" not in entity
    assert "service_compute_pool" not in entity
