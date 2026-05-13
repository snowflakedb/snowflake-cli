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

Scope: this module verifies the end-to-end account-parameter resolution path
that *only* a real Snowflake account can exercise. The CLI calls
``SHOW PARAMETERS LIKE 'DEFAULT_SNOWFLAKE_APPS_%' IN USER`` (see
``SnowflakeAppManager.fetch_snow_apps_parameters``) and the source label
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

Shared constants and the session ``ALTER USER`` fixture live in
``tests_integration.apps.conftest``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from tests_integration.apps.conftest import (
    BUILD_EAI,
    COMPUTE_POOL,
    DATABASE,
    SCHEMA,
    WAREHOUSE,
)


@pytest.mark.integration
def test_setup_resolves_from_account_parameters(
    runner,
    temporary_working_directory,
):
    """All ``DEFAULT_SNOWFLAKE_APPS_*`` account parameters drive ``snow app setup``.

    No CLI flags for compute/db/warehouse are passed, so values must come from
    the USER ("account") parameters set by the autouse session fixture.
    Asserts every resolved field is reflected in the generated YAML and that
    the source label printed by the resolver is ``(account parameter)``.
    """
    result = runner.invoke_with_connection(
        ["app", "setup", "--app-name", "param_app", "--build-eai", BUILD_EAI]
    )
    assert result.exit_code == 0, result.output

    expected_source_lines = [
        f"database: {DATABASE}  (account parameter)",
        f"schema: {SCHEMA}  (account parameter)",
        f"warehouse: {WAREHOUSE}  (account parameter)",
        f"build_compute_pool: {COMPUTE_POOL}  (account parameter)",
        f"service_compute_pool: {COMPUTE_POOL}  (account parameter)",
    ]
    for expected in expected_source_lines:
        assert expected in result.output, result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    entity = content["entities"]["param_app"]
    assert entity["identifier"]["database"] == DATABASE
    assert entity["identifier"]["schema"] == SCHEMA
    assert entity["query_warehouse"] == WAREHOUSE
    assert entity["build_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["service_compute_pool"]["name"] == COMPUTE_POOL
