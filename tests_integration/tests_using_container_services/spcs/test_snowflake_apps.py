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
WAREHOUSE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE", "xsmall")
BUILD_EAI = "cli_test_integration"
ARTIFACT_REPO_NAME = "SNOW_APPS_DEFAULT_ARTIFACT_REPOSITORY"
IMAGE_REPO_NAME = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"

_ACCOUNT_PARAMS = {
    "DEFAULT_SNOWFLAKE_APPS_QUERY_WAREHOUSE": WAREHOUSE,
    "DEFAULT_SNOWFLAKE_APPS_BUILD_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_SERVICE_COMPUTE_POOL": COMPUTE_POOL,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": DATABASE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": "public",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _current_user(session) -> str:
    rows = session.execute_string("SELECT CURRENT_USER()")
    return rows[-1].fetchone()[0]


@pytest.fixture(scope="session")
def snowflake_apps_account_params(snowflake_session):
    """Set DEFAULT_SNOWFLAKE_APPS_* user parameters for the session.

    Also clears build_eai at the user level so account-level defaults
    (which may reference a nonexistent EAI) don't leak into deploy tests.

    Yields the parameter dict so tests can assert against expected values.
    Cleans up (UNSET) on teardown.
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

    yield dict(_ACCOUNT_PARAMS)

    all_keys = " ".join(
        list(_ACCOUNT_PARAMS.keys())
        + ["DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION"]
    )
    try:
        snowflake_session.execute_string(f"ALTER USER {user} UNSET {all_keys}")
    except Exception:
        pass


@pytest.fixture()
def snowflake_apps_setup(snowflake_session):
    """Create the schema (and image repo) needed by the deploy flow.

    Yields ``(schema_name, unique_suffix)`` so the test can patch
    ``snowflake.yml`` with deterministic, non-colliding identifiers.

    On teardown every resource created during the test is dropped.
    """
    uid = uuid.uuid4().hex[:8]
    schema_name = f"SNOW_APP_TEST_{uid}"
    service_name = "TEST_APP"
    build_job_name = "TEST_APP_BUILD_JOB"
    stage_name = "TEST_APP_CODE"

    snowflake_session.execute_string(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{schema_name};"
    )
    snowflake_session.execute_string(
        f"CREATE IMAGE REPOSITORY IF NOT EXISTS "
        f"{DATABASE}.{schema_name}.{IMAGE_REPO_NAME};"
    )

    yield schema_name, uid

    # Drop builder job services (named SPCS_APP_BUILDER_JOB_<query_id>)
    # that hold compute pool nodes after the build completes/fails.
    try:
        rows = snowflake_session.execute_string(
            f"SHOW SERVICES LIKE 'SPCS_APP_BUILDER%' IN SCHEMA {DATABASE}.{schema_name}"
        )
        for row in rows[-1]:
            name = row[1]
            try:
                snowflake_session.execute_string(
                    f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.{name}"
                )
            except Exception:
                pass
    except Exception:
        pass

    for stmt in [
        f"DROP APPLICATION SERVICE IF EXISTS {DATABASE}.{schema_name}.{service_name}",
        f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.{service_name}",
        f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.{build_job_name}",
        f"DROP STAGE IF EXISTS {DATABASE}.{schema_name}.{stage_name}",
        f"DROP ARTIFACT REPOSITORY IF EXISTS {DATABASE}.{schema_name}.{ARTIFACT_REPO_NAME}",
        f"DROP IMAGE REPOSITORY IF EXISTS {DATABASE}.{schema_name}.{IMAGE_REPO_NAME}",
        f"DROP SCHEMA IF EXISTS {DATABASE}.{schema_name}",
    ]:
        try:
            snowflake_session.execute_string(stmt)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_project_yml(alter_snowflake_yml, yml_path, schema_name):
    """Apply standard patches to the snowflake_apps fixture's snowflake.yml."""
    alter_snowflake_yml(yml_path, "entities.test_app.identifier.database", DATABASE)
    alter_snowflake_yml(yml_path, "entities.test_app.identifier.schema", schema_name)
    alter_snowflake_yml(
        yml_path, "entities.test_app.build_compute_pool.name", COMPUTE_POOL
    )
    alter_snowflake_yml(
        yml_path, "entities.test_app.service_compute_pool.name", COMPUTE_POOL
    )


def _deploy(runner, entity_id="test_app", extra_flags=None):
    """Run ``snow app deploy`` and return the CommandResult."""
    cmd = ["app", "deploy", "--entity-id", entity_id, "--verbose"]
    if extra_flags:
        cmd.extend(extra_flags)
    return runner.invoke_with_connection(cmd)


# ---------------------------------------------------------------------------
# Tests — setup
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_setup_creates_valid_yml_with_flags(
    runner,
    temporary_working_directory,
    snowflake_apps_account_params,
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
    snowflake_apps_account_params,
):
    """``snow app setup`` resolves values from DEFAULT_SNOWFLAKE_APPS_* user parameters."""
    result = runner.invoke_with_connection_json(
        ["app", "setup", "--app-name", "param_app", "--build-eai", BUILD_EAI]
    )
    assert result.exit_code == 0, result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    entity = content["entities"]["param_app"]
    assert entity["build_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["service_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["identifier"]["database"] == DATABASE


@pytest.mark.integration
def test_setup_dry_run_does_not_write_file(
    runner,
    temporary_working_directory,
    snowflake_apps_account_params,
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


# ---------------------------------------------------------------------------
# Tests — deploy
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_deploy_mutually_exclusive_phase_flags(
    runner,
    project_directory,
    alter_snowflake_yml,
    snowflake_apps_setup,
):
    """Passing two phase flags at once should fail with a clear error."""
    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        _patch_project_yml(alter_snowflake_yml, yml_path, schema_name)

        result = _deploy(runner, extra_flags=["--upload-only", "--build-only"])
        assert result.exit_code != 0
        assert "Only one of" in result.output
