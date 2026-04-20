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

"""Integration tests for ``snow __app setup`` and ``snow __app deploy``."""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from unittest import mock

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
    "DEFAULT_SNOWFLAKE_APPS_BUILD_EXTERNAL_ACCESS_INTEGRATION": BUILD_EAI,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_DATABASE": DATABASE,
    "DEFAULT_SNOWFLAKE_APPS_DESTINATION_SCHEMA": "public",
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def enable_snowflake_apps_feature_flag():
    """Enable the ENABLE_SNOWFLAKE_APPS feature flag for the duration of a test."""
    with (
        mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_enabled",
            return_value=True,
        ),
        mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_SNOWFLAKE_APPS.is_disabled",
            return_value=False,
        ),
    ):
        yield


@pytest.fixture(scope="session")
def snowflake_apps_account_params(snowflake_session):
    """Set DEFAULT_SNOWFLAKE_APPS_* user parameters for the session.

    Yields the parameter dict so tests can assert against expected values.
    Cleans up (UNSET) on teardown.
    """
    set_clauses = " ".join(f"{k}='{v}'" for k, v in _ACCOUNT_PARAMS.items())
    snowflake_session.execute_string(f"ALTER USER CURRENT_USER() SET {set_clauses}")

    yield dict(_ACCOUNT_PARAMS)

    unset_keys = " ".join(_ACCOUNT_PARAMS.keys())
    try:
        snowflake_session.execute_string(
            f"ALTER USER CURRENT_USER() UNSET {unset_keys}"
        )
    except Exception:
        pass


@pytest.fixture()
def unset_snowflake_apps_account_params(snowflake_session):
    """Ensure DEFAULT_SNOWFLAKE_APPS_* user parameters are NOT set.

    Used by negative tests that assert setup fails when no params are present.
    """
    unset_keys = " ".join(_ACCOUNT_PARAMS.keys())
    try:
        snowflake_session.execute_string(
            f"ALTER USER CURRENT_USER() UNSET {unset_keys}"
        )
    except Exception:
        pass
    yield


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
    """Run ``snow __app deploy`` and return the CommandResult."""
    cmd = ["__app", "deploy", "--entity-id", entity_id]
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
    enable_snowflake_apps_feature_flag,
):
    """``snow __app setup`` with explicit flags produces a valid snowflake.yml."""
    result = runner.invoke_with_connection(
        [
            "__app",
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
    assert "Initialized Snowflake App project" in result.output

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
    enable_snowflake_apps_feature_flag,
    snowflake_apps_account_params,
):
    """``snow __app setup`` resolves values from DEFAULT_SNOWFLAKE_APPS_* user parameters."""
    result = runner.invoke_with_connection_json(
        ["__app", "setup", "--app-name", "param_app"]
    )
    assert result.exit_code == 0, result.output

    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    assert yml_path.exists(), "snowflake.yml was not created"

    with open(yml_path) as fh:
        content = yaml.safe_load(fh)

    entity = content["entities"]["param_app"]
    assert entity["build_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["service_compute_pool"]["name"] == COMPUTE_POOL
    assert entity["build_eai"]["name"] == BUILD_EAI
    assert entity["identifier"]["database"] == DATABASE


@pytest.mark.integration
def test_setup_dry_run_does_not_write_file(
    runner,
    temporary_working_directory,
    enable_snowflake_apps_feature_flag,
):
    """``snow __app setup --dry-run`` prints config but does not create snowflake.yml."""
    result = runner.invoke_with_connection(
        [
            "__app",
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
    enable_snowflake_apps_feature_flag,
):
    """Running setup when snowflake.yml already exists skips initialization."""
    yml_path = Path(temporary_working_directory) / "snowflake.yml"
    yml_path.write_text("definition_version: '2'\nentities: {}\n")

    result = runner.invoke_with_connection(
        [
            "__app",
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
def test_setup_errors_when_missing_required(
    runner,
    temporary_working_directory,
    enable_snowflake_apps_feature_flag,
    unset_snowflake_apps_account_params,
):
    """``snow __app setup`` fails when required values are missing and no flags are passed."""
    result = runner.invoke_with_connection(
        ["__app", "setup", "--app-name", "fail_app"],
        catch_exceptions=True,
    )
    assert result.exit_code != 0
    assert "Missing" in result.output or "missing" in result.output.lower()


# ---------------------------------------------------------------------------
# Tests — deploy
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_setup_and_deploy_end_to_end(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    enable_snowflake_apps_feature_flag,
    snowflake_apps_setup,
):
    """End-to-end: patch a project, deploy, verify the service exists and has an endpoint."""
    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        _patch_project_yml(alter_snowflake_yml, yml_path, schema_name)

        with open(yml_path) as fh:
            patched = yaml.safe_load(fh)
        assert patched["entities"]["test_app"]["identifier"]["database"] == DATABASE
        assert patched["entities"]["test_app"]["identifier"]["schema"] == schema_name

        result = _deploy(runner)
        assert (
            result.exit_code == 0
        ), f"Deploy failed (exit_code={result.exit_code}):\n{result.output}"
        assert "App ready at" in result.output

        rows = snowflake_session.execute_string(
            f"SHOW SERVICES IN SCHEMA {DATABASE}.{schema_name}"
        )
        service_names = [row[1] for row in rows[-1]]
        assert (
            "TEST_APP" in service_names
        ), f"Expected TEST_APP in services, got: {service_names}"


@pytest.mark.integration
def test_deploy_idempotent_upgrade(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    enable_snowflake_apps_feature_flag,
    snowflake_apps_setup,
):
    """Deploying twice triggers an upgrade on the second run and still succeeds."""
    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        _patch_project_yml(alter_snowflake_yml, yml_path, schema_name)

        first = _deploy(runner)
        assert first.exit_code == 0, f"First deploy failed:\n{first.output}"
        assert "App ready at" in first.output

        second = _deploy(runner)
        assert second.exit_code == 0, f"Second deploy failed:\n{second.output}"
        assert (
            "Upgrading" in second.output or "App ready at" in second.output
        ), f"Expected upgrade or ready message, got:\n{second.output}"


@pytest.mark.integration
def test_deploy_upload_only(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    enable_snowflake_apps_feature_flag,
    snowflake_apps_setup,
):
    """``--upload-only`` uploads artifacts to the stage without building or deploying."""
    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        _patch_project_yml(alter_snowflake_yml, yml_path, schema_name)

        result = _deploy(runner, extra_flags=["--upload-only"])
        assert (
            result.exit_code == 0
        ), f"Upload-only failed (exit={result.exit_code}):\n{result.output}"
        assert "Artifacts uploaded" in result.output

        rows = snowflake_session.execute_string(
            f"LS @{DATABASE}.{schema_name}.TEST_APP_CODE"
        )
        file_names = [row[0] for row in rows[-1]]
        assert any(
            "Dockerfile" in f for f in file_names
        ), f"Expected Dockerfile in stage, got: {file_names}"
        assert any(
            "server.py" in f for f in file_names
        ), f"Expected server.py in stage, got: {file_names}"

        svc_rows = snowflake_session.execute_string(
            f"SHOW SERVICES IN SCHEMA {DATABASE}.{schema_name}"
        )
        svc_names = [row[1] for row in svc_rows[-1]]
        assert (
            "TEST_APP" not in svc_names
        ), "Service should not exist after --upload-only"


@pytest.mark.integration
def test_deploy_build_only_after_upload(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    enable_snowflake_apps_feature_flag,
    snowflake_apps_setup,
):
    """Chain ``--upload-only`` then ``--build-only``: build completes without deploying."""
    schema_name, uid = snowflake_apps_setup

    with project_directory("snowflake_apps") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        _patch_project_yml(alter_snowflake_yml, yml_path, schema_name)

        upload = _deploy(runner, extra_flags=["--upload-only"])
        assert upload.exit_code == 0, f"Upload failed:\n{upload.output}"

        build = _deploy(runner, extra_flags=["--build-only"])
        assert (
            build.exit_code == 0
        ), f"Build-only failed (exit={build.exit_code}):\n{build.output}"
        assert "Build completed" in build.output

        svc_rows = snowflake_session.execute_string(
            f"SHOW SERVICES IN SCHEMA {DATABASE}.{schema_name}"
        )
        svc_names = [row[1] for row in svc_rows[-1]]
        assert (
            "TEST_APP" not in svc_names
        ), "Service should not exist after --build-only"


@pytest.mark.integration
def test_deploy_mutually_exclusive_phase_flags(
    runner,
    project_directory,
    alter_snowflake_yml,
    enable_snowflake_apps_feature_flag,
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
