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
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
import yaml

from snowflake.cli._plugins.dbt.constants import ENV_FILENAME, PROFILES_FILENAME
from snowflake.cli.api.identifiers import FQN


def _setup_dbt_profile(root_dir: Path, snowflake_session):
    with open((root_dir / PROFILES_FILENAME), "r") as f:
        profiles = yaml.safe_load(f)
    dev_profile = profiles["dbt_integration_project"]["outputs"]["dev"]
    dev_profile["database"] = snowflake_session.database
    dev_profile["role"] = snowflake_session.role
    dev_profile["schema"] = snowflake_session.schema
    dev_profile["type"] = "snowflake"

    prod_profile = dev_profile.copy()
    prod_profile["schema"] = f"{snowflake_session.schema}_PROD"
    profiles["dbt_integration_project"]["outputs"]["prod"] = prod_profile

    (root_dir / PROFILES_FILENAME).write_text(yaml.dump(profiles))


def _assert_default_target(name, runner, default_target):
    result = runner.invoke_with_connection_json(["dbt", "list", "--like", name.upper()])
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    if default_target is None:
        assert result.json[0]["default_target"] is None
    else:
        assert result.json[0]["default_target"].lower() == default_target


def _assert_default_environment(name, runner, default_environment):
    result = runner.invoke_with_connection_json(["dbt", "list", "--like", name.upper()])
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    row = result.json[0]
    if default_environment is None:
        assert row.get("default_environment") in (None, "")
    else:
        assert row["default_environment"].lower() == default_environment.lower()


def _fetch_creation_date(name, runner) -> datetime.datetime:
    result = runner.invoke_with_connection_json(
        [
            "dbt",
            "list",
            "--like",
            name,
        ]
    )
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    dbt_object = result.json[0]
    assert dbt_object["name"].lower() == name.lower()
    return datetime.datetime.fromisoformat(dbt_object["created_on"])


def _verify_dbt_project_exists(runner, name: str):
    """Verify that a dbt project exists and return its details."""
    result = runner.invoke_with_connection_json(["dbt", "list", "--like", name])
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    dbt_object = result.json[0]
    assert dbt_object["name"].lower() == name.lower()
    return dbt_object


def _assert_dbt_version(name, runner, dbt_version):
    result = runner.invoke_with_connection_json(["dbt", "list", "--like", name.upper()])
    assert result.exit_code == 0, result.output
    assert len(result.json) == 1
    if dbt_version is None:
        assert result.json[0].get("dbt_version") is None
    else:
        assert result.json[0]["dbt_version"] == dbt_version


def _setup_external_access_integration(runner, integration_name: str):
    """Create external access integration for dbt hub access."""
    network_rule_name = f"{integration_name.upper()}_NETWORK_RULE"

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
    network_rule_name = f"{integration_name.upper()}_NETWORK_RULE"

    # Drop external access integration
    runner.invoke_with_connection_json(
        ["sql", "-q", f"DROP EXTERNAL ACCESS INTEGRATION IF EXISTS {integration_name}"]
    )
    # Don't assert on exit code as cleanup should be non-blocking

    # Drop network rule
    runner.invoke_with_connection_json(
        ["sql", "-q", f"DROP NETWORK RULE IF EXISTS {network_rule_name}"]
    )
    # Don't assert on exit code as cleanup should be non-blocking


@contextmanager
def no_db_schema_connection_context(runner):
    runner.use_config("connection_configs_no_db_schema.toml")

    env = dict(os.environ)
    env.pop("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", None)
    env.pop("SNOWFLAKE_CONNECTIONS_INTEGRATION_SCHEMA", None)

    try:
        with mock.patch.dict(os.environ, env, clear=True):
            yield
    finally:
        runner.use_config("connection_configs.toml")


@pytest.mark.integration
def test_deploy_and_execute(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"

        # deploy for the first time
        _setup_dbt_profile(root_dir, snowflake_session)
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
def test_command_aliases(
    runner,
    snowflake_session,
    test_database,
    project_directory,
    snapshot,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}".upper()
        _setup_dbt_profile(root_dir, snowflake_session)

        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(["dbt", "describe", name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(["dbt", "drop", name])
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(["dbt", "list"])
        assert result.exit_code == 0, result.output
        assert len(result.json) == 0, result.json


@pytest.mark.integration
def test_deploy_and_execute_with_full_fqn(
    runner,
    snowflake_session,
    test_database,
    project_directory,
    snapshot,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        # Create alternative schema that differs from connection default
        alt_schema = f"{snowflake_session.schema}_FQN_TEST_{ts}"
        snowflake_session.execute_string(f"CREATE SCHEMA {alt_schema}")

        # Given a local dbt project
        name = f"dbt_project_{ts}"
        fqn = FQN.from_string(f"{snowflake_session.database}.{alt_schema}.{name}")

        _setup_dbt_profile(root_dir, snowflake_session)

        # All operations should succeed when no db and schema context in the connection,
        # relying solely on the FQN for database/schema resolution
        with no_db_schema_connection_context(runner):
            result = runner.invoke_with_connection_json(["dbt", "deploy", str(fqn)])
            assert result.exit_code == 0, result.output

            # call `run` on dbt object
            result = runner.invoke_passthrough_with_connection(
                args=[
                    "dbt",
                    "execute",
                ],
                passthrough_args=[str(fqn), "run"],
            )

            # a successful execution should produce data in my_second_dbt_model and
            assert result.exit_code == 0, result.output
            assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2" in result.output

            result = runner.invoke_with_connection_json(
                [
                    "sql",
                    "-q",
                    f"select count(*) as COUNT from {fqn.database}.{alt_schema}.my_second_dbt_model;",
                ]
            )
            assert len(result.json) == 1, result.json
            assert result.json[0]["COUNT"] == 1, result.json[0]


@pytest.mark.integration
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
        _setup_dbt_profile(root_dir, snowflake_session)
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


@pytest.mark.integration
def test_deploy_with_default_target(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_default_target_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)

        result = runner.invoke_with_connection(
            [
                "sql",
                "-q",
                f"create schema if not exists {snowflake_session.schema}_PROD",
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--default-target", "prod"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_target(name, runner, "prod")

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--default-target", "dev"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_target(name, runner, "dev")

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--unset-default-target"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_target(name, runner, None)


@pytest.mark.integration
@pytest.mark.qa_only
def test_deploy_with_default_environment(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    """Exercise --default-env / --unset-default-env lifecycle."""
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_default_env_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)

        # Stage env.yml inside the project source so the server has something to
        # match the DEFAULT_ENVIRONMENT against.
        env_yml = {
            "env_config": {
                "default_environment": "dev",
                "environments": [
                    {"name": "dev", "env": {"DBT_FOO": "bar"}},
                    {"name": "prod", "env": {"DBT_FOO": "baz"}},
                ],
            }
        }
        (root_dir / ENV_FILENAME).write_text(yaml.dump(env_yml))

        # 1. CREATE path: deploy with --default-env=dev
        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--default-env", "dev"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_environment(name, runner, "dev")

        # 2. ALTER SET path: change the default environment on existing object
        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--default-env", "prod"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_environment(name, runner, "prod")

        # 3. ALTER UNSET path: clear the default environment
        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--unset-default-env"]
        )
        assert result.exit_code == 0, result.output
        _assert_default_environment(name, runner, None)


@pytest.mark.integration
@pytest.mark.qa_only
def test_deploy_with_env_file_dir(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    """Exercise --env-file-dir injection."""
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_env_file_dir_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)

        # env.yml lives outside the project source — analogous to --profiles-dir.
        env_dir = Path(root_dir).parent / "envs"
        env_dir.mkdir(parents=True, exist_ok=True)
        env_yml = {
            "env_config": {
                "default_environment": "dev",
                "environments": [
                    {"name": "dev", "env": {"DBT_FOO": "bar"}},
                ],
            }
        }
        (env_dir / ENV_FILENAME).write_text(yaml.dump(env_yml))

        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--env-file-dir",
                str(env_dir.resolve()),
                "--default-env",
                "dev",
            ]
        )
        assert result.exit_code == 0, result.output
        _assert_default_environment(name, runner, "dev")


@pytest.mark.integration
def test_execute_with_target(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_target_{ts}"
        second_target_schema = f"{snowflake_session.schema}_PROD"
        snowflake_session.execute_string(
            f"create schema {second_target_schema}; use schema PUBLIC"
        )

        _setup_dbt_profile(root_dir, snowflake_session)

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--default-target=dev"]
        )
        assert result.exit_code == 0, result.output

        # execute on implicit default target
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run"],
        )

        assert result.exit_code == 0, result.output
        assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2" in result.output

        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                f"select count(*) as COUNT from {snowflake_session.database}.{snowflake_session.schema}.my_second_dbt_model;",
            ]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["COUNT"] == 1, result.json[0]

        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                f"select count(*) as COUNT from {snowflake_session.database}.{second_target_schema}.my_second_dbt_model;",
            ]
        )
        # Should fail because table doesn't exist in prod schema
        assert result.exit_code == 1, "Table should not exist in prod schema yet"
        assert (
            "does not exist" in result.output.lower()
            or "object does not exist" in result.output.lower()
        )

        # Now execute with explicit target=prod
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run", "--target=prod"],
        )

        assert result.exit_code == 0, result.output
        assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2" in result.output

        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                f"select count(*) as COUNT from {snowflake_session.database}.{second_target_schema}.my_second_dbt_model;",
            ]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["COUNT"] == 1, result.json[0]


@pytest.mark.integration
@pytest.mark.qa_only
def test_dbt_deploy_with_external_access_integrations(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project_with_external_deps") as root_dir:
        # Given a local dbt project with external dependencies
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_external_deps_{ts}"
        ext_access_integration = f"DBT_HUB_ACCESS_INTEGRATION"

        # Setup external access integration for dbt hub access
        _setup_external_access_integration(runner, ext_access_integration)

        _setup_dbt_profile(root_dir, snowflake_session)

        # Deploy dbt project with external access integrations
        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--external-access-integration",
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

        # Deploy dbt project once again to confirm that altering works
        second_access_integration = f"SECOND_ACCESS_INTEGRATION"

        # Setup external access integration for dbt hub access
        _setup_external_access_integration(runner, second_access_integration)

        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                name,
                "--external-access-integration",
                ext_access_integration,
                "--external-access-integration",
                second_access_integration,
            ]
        )
        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(["dbt", "describe", name])
        assert ext_access_integration in result.json[0]["external_access_integrations"]
        assert (
            second_access_integration in result.json[0]["external_access_integrations"]
        )

        # Cleanup: Remove external access integration and network rule
        _cleanup_external_access_integration(runner, ext_access_integration)
        _cleanup_external_access_integration(runner, second_access_integration)


@pytest.mark.integration
def test_deploy_project_with_local_deps(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project_with_local_deps") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"
        fqn = FQN.from_string(
            f"{snowflake_session.database}.{snowflake_session.schema}.{name}"
        )

        _setup_dbt_profile(root_dir, snowflake_session)
        result = runner.invoke_with_connection_json(
            [
                "dbt",
                "deploy",
                str(fqn),
                "--install-local-deps",
            ]
        )
        assert result.exit_code == 0, result.output

        # Verify the dbt project was created
        _verify_dbt_project_exists(runner, name)

        result = runner.invoke_passthrough_with_connection(
            args=["dbt", "execute"], passthrough_args=[name, "run"]
        )
        assert result.exit_code == 0, result.output

        # Make sure that uppercasing macro was used
        result = runner.invoke_with_connection_json(
            [
                "sql",
                "-q",
                f"select uppercase_name from {snowflake_session.database}.{snowflake_session.schema}.first_model_with_local;",
            ]
        )
        assert result.exit_code == 0, result.output
        assert all(
            map(lambda x: x["UPPERCASE_NAME"].isupper(), result.json)
        ), result.json


@pytest.mark.integration
def test_execute_with_variables(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project_with_variables") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"

        # deploy for the first time
        _setup_dbt_profile(root_dir, snowflake_session)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        # 1. run and provide one variable with simplified syntax.
        # The other variable should default to the value from dbt_project.yml
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run", "--vars", "env: local"],
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select env_tmpl, user_tmpl from my_second_dbt_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["ENV_TMPL"] == "local", result.json[0]
        assert result.json[0]["USER_TMPL"] == "default", result.json[0]

        # 2. run and provide both variables without double quotes
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run", "--vars", "{env: stage, user: stage_user}"],
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select env_tmpl, user_tmpl from my_second_dbt_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["ENV_TMPL"] == "stage", result.json[0]
        assert result.json[0]["USER_TMPL"] == "stage_user", result.json[0]

        # 3. run and provide both variables wrapped with double quotes
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[
                name,
                "run",
                "--vars",
                '{"env": "prod", "user": "prod_user"}',
            ],
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select env_tmpl, user_tmpl from my_second_dbt_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["ENV_TMPL"] == "prod", result.json[0]
        assert result.json[0]["USER_TMPL"] == "prod_user", result.json[0]


@pytest.mark.integration
def test_deploy_with_dbt_version(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_version_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--dbt-version", "1.9.4"]
        )
        assert result.exit_code == 0, result.output
        _assert_dbt_version(name, runner, "1.9.4")

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--dbt-version", "1.10.15"]
        )
        assert result.exit_code == 0, result.output
        _assert_dbt_version(name, runner, "1.10.15")


@pytest.mark.integration
def test_execute_with_dbt_version(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_exec_version_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)

        result = runner.invoke_with_connection_json(
            ["dbt", "deploy", name, "--dbt-version", "1.9.4"]
        )
        assert result.exit_code == 0, result.output
        _assert_dbt_version(name, runner, "1.9.4")

        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
                "--dbt-version=1.10.15",
            ],
            passthrough_args=[name, "run"],
        )

        assert result.exit_code == 0, result.output
        assert "Running with dbt=1.10.15" in result.output
        assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2" in result.output


@pytest.mark.integration
@pytest.mark.qa_only
def test_execute_with_env_and_env_vars(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    """Exercise --env and --env-vars against a real Snowflake DBT PROJECT.

    The fixture model reads DBT_FOO and DBT_BAR via dbt env_var() with a
    "unset" default. env.yml defines two environments (dev, prod), each
    setting both vars. We verify that:
      * --env=dev applies the dev block
      * --env=prod applies the prod block
      * --env=prod --env-vars '{"DBT_FOO": "..."}' overrides DBT_FOO while
        DBT_BAR still comes from env.yml
      * --env=NO_ENV skips env.yml so env_var() falls back to the default
    """
    with project_directory("dbt_project_with_env_vars") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_env_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        def _run_and_fetch(extra_args: list[str]) -> dict:
            run_result = runner.invoke_passthrough_with_connection(
                args=["dbt", "execute", *extra_args],
                passthrough_args=[name, "run"],
            )
            assert run_result.exit_code == 0, run_result.output

            query_result = runner.invoke_with_connection_json(
                ["sql", "-q", "select foo, bar from my_first_dbt_model;"]
            )
            assert query_result.exit_code == 0, query_result.output
            assert len(query_result.json) == 1, query_result.json
            return query_result.json[0]

        # 1. --env=dev → dev block from env.yml
        row = _run_and_fetch(["--env=dev"])
        assert row["FOO"] == "dev_foo", row
        assert row["BAR"] == "dev_bar", row

        # 2. --env=prod → prod block from env.yml
        row = _run_and_fetch(["--env=prod"])
        assert row["FOO"] == "prod_foo", row
        assert row["BAR"] == "prod_bar", row

        # 3. --env-vars overrides one key from the prod block
        row = _run_and_fetch(
            ["--env=prod", "--env-vars", '{"DBT_FOO": "override_foo"}']
        )
        assert row["FOO"] == "override_foo", row
        assert row["BAR"] == "prod_bar", row

        # 4. --env=NO_ENV skips env.yml → env_var() default fires
        row = _run_and_fetch(["--env=NO_ENV"])
        assert row["FOO"] == "unset", row
        assert row["BAR"] == "unset", row

        # 5. --env-vars without --env still applies (and env.yml's
        # default_environment 'dev' provides the un-overridden var)
        row = _run_and_fetch(["--env-vars", '{"DBT_FOO": "only_foo"}'])
        assert row["FOO"] == "only_foo", row
        assert row["BAR"] == "dev_bar", row


@pytest.mark.integration
@pytest.mark.qa_only
def test_execute_with_use_shell_env_vars(
    runner,
    snowflake_session,
    test_database,
    project_directory,
    monkeypatch,
):
    """Exercise --use-shell-env-vars against a real Snowflake DBT PROJECT.

    Reuses the sibling's `dbt_project_with_env_vars` fixture project. The
    integration runner uses an in-process CliRunner, so monkeypatch.setenv
    is visible to os.environ inside the CLI code.

    Verifies:
      * shell DBT_FOO/DBT_BAR forwarded → overrides env.yml's prod block
      * shell + --env-vars → explicit --env-vars wins on collision
      * shell + --env=NO_ENV → shell vars apply, env.yml skipped
      * DBT_ENV_SECRET_* in shell → silently dropped client-side
      * empty shell + --use-shell-env-vars → env.yml provides values
    """
    # Strip any stray DBT_* leakage from the runner's env first.
    for k in list(os.environ):
        if k.startswith("DBT_"):
            monkeypatch.delenv(k, raising=False)

    with project_directory("dbt_project_with_env_vars") as root_dir:
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_shell_env_{ts}"

        _setup_dbt_profile(root_dir, snowflake_session)
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        def _run_and_fetch(extra_args: list[str]) -> dict:
            run_result = runner.invoke_passthrough_with_connection(
                args=["dbt", "execute", *extra_args],
                passthrough_args=[name, "run"],
            )
            assert run_result.exit_code == 0, run_result.output

            query_result = runner.invoke_with_connection_json(
                ["sql", "-q", "select foo, bar from my_first_dbt_model;"]
            )
            assert query_result.exit_code == 0, query_result.output
            assert len(query_result.json) == 1, query_result.json
            return query_result.json[0]

        # 1. shell vars forwarded; override env.yml's prod block.
        monkeypatch.setenv("DBT_FOO", "shell_foo")
        monkeypatch.setenv("DBT_BAR", "shell_bar")
        row = _run_and_fetch(["--use-shell-env-vars", "--env=prod"])
        assert row["FOO"] == "shell_foo", row
        assert row["BAR"] == "shell_bar", row

        # 2. --env-vars beats shell on collision; shell still provides BAR.
        row = _run_and_fetch(
            [
                "--use-shell-env-vars",
                "--env-vars",
                '{"DBT_FOO": "explicit_foo"}',
                "--env=prod",
            ]
        )
        assert row["FOO"] == "explicit_foo", row
        assert row["BAR"] == "shell_bar", row

        # 3. --env=NO_ENV skips env.yml; shell still forwards.
        monkeypatch.delenv("DBT_BAR", raising=False)
        row = _run_and_fetch(["--use-shell-env-vars", "--env=NO_ENV"])
        assert row["FOO"] == "shell_foo", row
        assert row["BAR"] == "unset", row

        # 4. DBT_ENV_SECRET_* dropped client-side; doesn't reach the server.
        monkeypatch.setenv("DBT_ENV_SECRET_TOKEN", "should_not_appear")
        run_result = runner.invoke_passthrough_with_connection(
            args=["dbt", "execute", "--use-shell-env-vars", "--env=NO_ENV"],
            passthrough_args=[name, "run"],
        )
        assert run_result.exit_code == 0, run_result.output
        assert "should_not_appear" not in run_result.output
        monkeypatch.delenv("DBT_ENV_SECRET_TOKEN", raising=False)

        # 5. Empty shell + --use-shell-env-vars → env.yml provides values,
        # pipeline succeeds with the empty-state info message.
        monkeypatch.delenv("DBT_FOO", raising=False)
        row = _run_and_fetch(["--use-shell-env-vars", "--env=prod"])
        assert row["FOO"] == "prod_foo", row
        assert row["BAR"] == "prod_bar", row
