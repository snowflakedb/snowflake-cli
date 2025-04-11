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

import pytest
import yaml


@pytest.mark.integration
@pytest.mark.qa_only
def test_dbt(
    runner,
    snowflake_session,
    test_database,
    project_directory,
):
    with project_directory("dbt_project") as root_dir:
        # Given a local dbt project
        ts = int(datetime.datetime.now().timestamp())
        name = f"dbt_project_{ts}"
        _setup_dbt_profile(root_dir, snowflake_session)

        # When it's deployed
        result = runner.invoke_with_connection_json(["dbt", "deploy", name])
        assert result.exit_code == 0, result.output

        # Then it can be listed
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

        # And when dbt run gets called on it
        result = runner.invoke_passthrough_with_connection(
            args=[
                "dbt",
                "execute",
            ],
            passthrough_args=[name, "run"],
        )

        # Then is succeeds and models get populated according to expectations
        assert result.exit_code == 0, result.output
        assert "Done. PASS=2 WARN=0 ERROR=0 SKIP=0 TOTAL=2" in result.output

        result = runner.invoke_with_connection_json(
            ["sql", "-q", "select count(*) as COUNT from my_second_dbt_model;"]
        )
        assert len(result.json) == 1, result.json
        assert result.json[0]["COUNT"] == 1, result.json[0]


def _setup_dbt_profile(root_dir, snowflake_session):
    with open((root_dir / "profiles.yml"), "r") as f:
        profiles = yaml.safe_load(f)
    dev_profile = profiles["dbt_integration_project"]["outputs"]["dev"]
    dev_profile["database"] = snowflake_session.database
    dev_profile["account"] = snowflake_session.account
    dev_profile["user"] = snowflake_session.user
    dev_profile["role"] = snowflake_session.role
    dev_profile["warehouse"] = snowflake_session.warehouse
    dev_profile["schema"] = snowflake_session.schema
    (root_dir / "profiles.yml").write_text(yaml.dump(profiles))
