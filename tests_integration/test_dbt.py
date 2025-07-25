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

from snowflake.cli.api.identifiers import FQN
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

        # list all dbt objects
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
def test_deploy_and_execute_with_full_fqn(
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
        fqn = FQN.from_string(
            f"{snowflake_session.database}.{snowflake_session.schema}.{name}"
        )

        _setup_dbt_profile(root_dir, snowflake_session, include_password=False)
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
                f"select count(*) as COUNT from {fqn.database}.{fqn.schema}.my_second_dbt_model;",
            ]
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
