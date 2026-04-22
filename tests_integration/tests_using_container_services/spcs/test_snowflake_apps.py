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

"""Integration tests for ``snow app setup`` and ``snow app deploy``
against ``snowflake-app`` (Snowflake Apps Deploy) entities."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
import yaml

COMPUTE_POOL = "snowcli_compute_pool"
DATABASE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB")
IMAGE_REPO_NAME = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def snowflake_apps_setup(snowflake_session):
    """Create the image repository and unique schema needed by the deploy flow.

    Yields ``(schema_name, unique_suffix)`` so the test can patch
    ``snowflake.yml`` with deterministic, non-colliding identifiers.

    On teardown every resource created during the test is dropped.
    """
    uid = uuid.uuid4().hex[:8]
    schema_name = f"SNOW_APP_TEST_{uid}"
    service_name = f"TEST_APP"
    build_job_name = f"TEST_APP_BUILD_JOB"
    stage_name = f"TEST_APP_CODE"

    # Pre-create schema and image repo so deploy can find them.
    snowflake_session.execute_string(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{schema_name};"
    )
    snowflake_session.execute_string(
        f"CREATE IMAGE REPOSITORY IF NOT EXISTS "
        f"{DATABASE}.{schema_name}.{IMAGE_REPO_NAME};"
    )

    yield schema_name, uid

    # ── Teardown: best-effort cleanup ──────────────────────────────────
    for stmt in [
        f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.{service_name}",
        f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.{build_job_name}",
        f"DROP STAGE IF EXISTS {DATABASE}.{schema_name}.{stage_name}",
        f"DROP IMAGE REPOSITORY IF EXISTS {DATABASE}.{schema_name}.{IMAGE_REPO_NAME}",
        f"DROP SCHEMA IF EXISTS {DATABASE}.{schema_name}",
    ]:
        try:
            snowflake_session.execute_string(stmt)
        except Exception:
            pass  # best-effort cleanup


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_snowflake_apps_setup_and_deploy(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    snowflake_apps_setup,
):
    """End-to-end: init a project, patch it, deploy, verify the service exists."""

    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        # ── Patch snowflake.yml with test-specific identifiers ────────
        yml_path = Path(project_dir) / "snowflake.yml"
        alter_snowflake_yml(yml_path, "entities.test_app.identifier.database", DATABASE)
        alter_snowflake_yml(
            yml_path, "entities.test_app.identifier.schema", schema_name
        )
        alter_snowflake_yml(
            yml_path,
            "entities.test_app.build_compute_pool.name",
            COMPUTE_POOL,
        )
        alter_snowflake_yml(
            yml_path,
            "entities.test_app.service_compute_pool.name",
            COMPUTE_POOL,
        )

        # Verify the YAML is valid after patching
        with open(yml_path) as fh:
            patched = yaml.safe_load(fh)
        assert patched["entities"]["test_app"]["identifier"]["database"] == DATABASE
        assert patched["entities"]["test_app"]["identifier"]["schema"] == schema_name

        # ── Deploy ────────────────────────────────────────────────────
        result = runner.invoke_with_connection(
            ["app", "deploy", "--entity-id", "test_app"]
        )
        assert (
            result.exit_code == 0
        ), f"Deploy failed (exit_code={result.exit_code}):\n{result.output}"
        assert "App ready at" in result.output

        # ── Verify service exists via SQL ─────────────────────────────
        rows = snowflake_session.execute_string(
            f"SHOW SERVICES IN SCHEMA {DATABASE}.{schema_name}"
        )
        service_names = [row[1] for row in rows[-1]]  # "name" is second column
        assert (
            "TEST_APP" in service_names
        ), f"Expected TEST_APP in services, got: {service_names}"


@pytest.mark.integration
def test_snowflake_apps_setup_creates_valid_yml(
    runner,
    temporary_working_directory,
):
    """``snow app setup`` should produce a valid snowflake.yml."""

    result = runner.invoke_with_connection(
        ["app", "setup", "--app-name", "my_test_app"]
    )
    assert result.exit_code == 0, result.output
    assert "Initialized Snowflake Apps Deploy project" in result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    assert content["definition_version"] == "2"
    assert "my_test_app" in content["entities"]
    assert content["entities"]["my_test_app"]["type"] == "snowflake-app"
