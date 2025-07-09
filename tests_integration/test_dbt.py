# Copyright (c) 2025 Snowflake Inc.
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
import datetime
import os
from pathlib import Path

import pytest
import yaml

from snowflake.cli._plugins.dbt.constants import PROFILES_FILENAME


@pytest.mark.integration
@pytest.mark.qa_only
def test_deploy_and_execute(
    runner,
    snowflake_session,
    test_database,
    project_directory,
    snapshot,
):
    with project_directory("dbt_project") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"

        # try to deploy, but fail since profiles.yml contains a password
        _setup_dbt_profile(root_dir, snowflake_session, include_password=True)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 1, result.output
        assert result.output == snapshot

        # deploy for the first time
        _setup_dbt_profile(root_dir, snowflake_session, include_password=False)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        # change location of profiles.yml and redeploy
        new_profiles_directory = Path(root_dir) / "dbt_profiles"
        new_profiles_directory.mkdir(parents=True, exist_ok=True)
        profiles_file = root_dir / PROFILES_FILENAME
        profiles_file.rename(new_profiles_directory / PROFILES_FILENAME)

        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--profiles-dir",
                str(new_profiles_directory.resolve()),
            ]
        )
        assert result.exit_code == 0, result.output

        _verify_dbt_project_exists(runner, name)

        # call `run` on dbt object
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run"],
        )

        # a successful execution should produce data in my_second_dbt_model and
        assert result.exit_code == 0, result.output
        assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2" in result.output

        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select count(*) as COUNT from my_second_dbt_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["COUNT"] == 1, result.json[0]


@pytest.mark.integration
@pytest.mark.qa_only
def test_dbt_deploy_options(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"

        # deploy for the first time - create new dbt object
        _setup_dbt_profile(root_dir, snowflake_session, include_password=False)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        timestamp_after_create = _fetch_creation_date(name, runner)

        # deploy for the second time - alter existing object and use profiles.yml as symlink
        profiles_file = (Path(root_dir) / PROFILES_FILENAME).rename(
            Path(root_dir) / "profiles_file"
        )
        os.symlink(profiles_file, Path(root_dir) / PROFILES_FILENAME)
        assert (Path(root_dir) / PROFILES_FILENAME).is_symlink()

        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        timestamp_after_alter = _fetch_creation_date(name, runner)
        assert (
            timestamp_after_alter == timestamp_after_create
        ), f"Timestamps differ: {timestamp_after_alter} vs {timestamp_after_create}"

        # deploy for the third time - this time with --force flag to replace dbt object
        result = runner.invoke_with_connection_json(["dbt", "deploy", name, "--force"])
        assert result.exit_code == 0, result.output

        timestamp_after_replace = _fetch_creation_date(name, runner)
        assert (
            timestamp_after_replace > timestamp_after_create
        ), f"Timestamps are the same: {timestamp_after_replace} vs {timestamp_after_create}"


def _fetch_creation_date(name, runner) -> datetime.datetime:
    dbt_object = _verify_dbt_project_exists(runner, name)
    return datetime.datetime.fromisoformat(dbt_object["created_on"])


def _setup_dbt_profile(root_dir: Path, snowflake_session, include_password: bool):
    with open((root_dir / PROFILES_FILENAME), "r") as f:
        profiles = yaml.safe_load(f)
    dev_profile = profiles["dbt_integration_project"]["outputs"]["dev"]
    dev_profile["database"] = snowflake_session.database
    dev_profile["account"] = snowflake_session.account
    dev_profile["user"] = snowflake_session.user
    dev_profile["role"] = snowflake_session.role
    dev_profile["warehouse"] = snowflake_session.warehouse
    dev_profile["schema"] = snowflake_session.schema
    if include_password:
        dev_profile["password"] = "secret_phrase"
    else:
        dev_profile.pop("password", None)
    (root_dir / PROFILES_FILENAME).write_text(yaml.dump(profiles))


def _verify_dbt_project_exists(runner, name: str):
    """Verify that a dbt project exists and return its details."""
    result = runner.invoke_with_connection_json(["dbt", "list", "--like", name])
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    dbt_object = result.json[0]
    assert dbt_object["name"].lower() == name.lower()
    return dbt_object


@pytest.mark.skipif(True, reason="Skipping this test for now")
@pytest.mark.integration
@pytest.mark.qa_only
def test_dbt_deploy_with_external_access_integrations(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    """
    Test dbt deploy with external access integrations for installing external dependencies.

    This test verifies that:
    1. dbt projects can be deployed with external access integrations
    2. External dependencies can be automatically installed
    3. Models using external package macros work correctly
    """
    with project_directory("dbt_project_with_external_deps") as root_dir:
        # Given a local dbt project with external dependencies
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_external_deps_{ts}"
        ext_access_integration = f"DBT_HUB_ACCESS_INTEGRATION"

        # Setup external access integration for dbt hub access
        _setup_external_access_integration(runner, ext_access_integration)

        _setup_dbt_profile(root_dir, snowflake_session, include_password=False)

        # Deploy dbt project with external access integrations
        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--external-access-integrations",
                ext_access_integration,
            ]
        )
        assert result.exit_code == 0, result.output

        # Verify the dbt project was created
        _verify_dbt_project_exists(runner, name)

        # Run the dbt models that use external package macros
        result = runner.invoke_passthrough_with_connection(
            args=["dbt", "execute"], passthrough_args=[name, "run"]
        )
        assert result.exit_code == 0, result.output

        # Verify surrogate key was generated (using dbt_utils macro)
        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                "select surrogate_key from first_model_with_utils where id = 1;",
            ]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["SURROGATE_KEY"] is not None, result.json[0]

        # Now add a different external dependency and update the project
        _add_second_dependency(root_dir)

        # Redeploy with the updated dependencies
        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--external-access-integrations",
                ext_access_integration,
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_passthrough_with_connection(
            args=["dbt", "execute"], passthrough_args=[name, "run"]
        )
        assert result.exit_code == 0, result.output

        # Verify the new model created data
        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select count(*) as COUNT from third_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["COUNT"] == 2, result.json[0]

        # Cleanup: Remove external access integration and network rule
        _cleanup_external_access_integration(runner, ext_access_integration)


def _setup_external_access_integration(runner, integration_name: str):
    """Create external access integration for dbt hub access."""
    network_rule_name = f"{integration_name}_NETWORK_RULE"

    # Create network rule for dbt hub and GitHub access
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            f"""
        CREATE OR REPLACE NETWORK RULE {network_rule_name}
          MODE = EGRESS
          TYPE = HOST_PORT
          VALUE_LIST = (
            'hub.getdbt.com',
            'codeload.github.com'
          )
        """,
        ]
    )
    assert result.exit_code == 0, result.output

    # Create external access integration using the network rule
    result = runner.invoke_with_connection_json(
        [
            "sql",
            "-q",
            f"""
        CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {integration_name}
          ALLOWED_NETWORK_RULES = ({network_rule_name})
          ENABLED = true
        """,
        ]
    )
    assert result.exit_code == 0, result.output


def _cleanup_external_access_integration(runner, integration_name: str):
    """Clean up external access integration and network rule."""
    network_rule_name = f"{integration_name}_network_rule"

    # Drop external access integration
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"DROP EXTERNAL ACCESS INTEGRATION IF EXISTS {integration_name}"]
    )
    # Don't assert on exit code as cleanup should be non-blocking

    # Drop network rule
    result = runner.invoke_with_connection_json(
        ["sql", "-q", f"DROP NETWORK RULE IF EXISTS {network_rule_name}"]
    )
    # Don't assert on exit code as cleanup should be non-blocking


def _add_second_dependency(root_dir: Path):
    """Add another dependency to the dbt project and a new model that uses it."""
    packages_content = """packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: dbt-labs/audit_helper
    version: 0.9.0
"""
    (root_dir / "packages.yml").write_text(packages_content)

    new_model_content = """{{ config(materialized='table') }}

with source_data as (
    select 1 as id, 'old_value' as name
    union all
    select 2 as id, 'updated_value' as name
),

updated_data as (
    select 1 as id, 'new_value' as name
    union all
    select 2 as id, 'updated_value' as name
)

select 
    id,
    name,
    'audit_comparison' as audit_type
from source_data
"""
    models_dir = root_dir / "models" / "example"
    models_dir.mkdir(parents=True, exist_ok=True)
    (models_dir / "third_model.sql").write_text(new_model_content)
