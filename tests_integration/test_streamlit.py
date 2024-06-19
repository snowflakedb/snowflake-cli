# Copyright (c) 2024 Snowflake Inc.
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

import uuid
from pathlib import Path

import pytest

from tests_integration.test_utils import (
    contains_row_with,
    row_from_snowflake_session,
    rows_from_snowflake_session,
)
from tests_integration.testing_utils import assert_that_result_is_successful


@pytest.mark.integration
def test_streamlit_deploy(
    runner,
    snowflake_session,
    test_database,
    _new_streamlit_role,
    project_directory,
):
    streamlit_name = "test_streamlit_deploy_snowcli"

    with project_directory("streamlit"):
        result = runner.invoke_with_connection_json(["streamlit", "deploy"])
        assert result.exit_code == 0

        result = runner.invoke_with_connection_json(["streamlit", "list"])
        assert_that_result_is_successful(result)

        expect = snowflake_session.execute_string(
            f"show streamlits like '{streamlit_name}'"
        )
        assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

        result = runner.invoke_with_connection_json(
            ["streamlit", "describe", streamlit_name]
        )
        expect = snowflake_session.execute_string(
            f"describe streamlit {streamlit_name}"
        )
        assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])
        assert contains_row_with(result.json, {"title": "My Fancy Streamlit"})

        result = runner.invoke_with_connection_json(
            ["streamlit", "get-url", streamlit_name]
        )

        assert result.json["message"].endswith(
            f"/#/streamlit-apps/{test_database.upper()}.PUBLIC.{streamlit_name.upper()}"
        )

        result = runner.invoke_with_connection_json(
            ["streamlit", "share", streamlit_name, _new_streamlit_role]
        )
        assert contains_row_with(
            result.json,
            {"status": "Statement executed successfully."},
        )
        expect = snowflake_session.execute_string(
            f"use role {_new_streamlit_role}; show streamlits like '{streamlit_name}'; use role integration_tests;"
        )
        assert contains_row_with(
            rows_from_snowflake_session(expect)[1], {"name": streamlit_name.upper()}
        )

    result = runner.invoke_with_connection_json(["streamlit", "drop", streamlit_name])
    assert contains_row_with(
        result.json,
        {"status": f"{streamlit_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(
        f"show streamlits like '{streamlit_name}'"
    )
    assert row_from_snowflake_session(expect) == []


@pytest.mark.integration
@pytest.mark.skip(
    reason="only works in accounts with experimental checkout behavior enabled"
)
def test_streamlit_deploy_experimental_twice(
    runner,
    snowflake_session,
    test_database,
    _new_streamlit_role,
    project_directory,
):
    streamlit_name = "test_streamlit_deploy_snowcli"

    with project_directory("streamlit"):
        result = runner.invoke_with_connection_json(
            ["streamlit", "deploy", "--experimental"]
        )
        assert result.exit_code == 0

        # Test that second deploy does not fail
        result = runner.invoke_with_connection_json(
            ["streamlit", "deploy", "--experimental"]
        )
        assert result.exit_code == 0

        result = runner.invoke_with_connection_json(["streamlit", "list"])
        assert_that_result_is_successful(result)

        expect = snowflake_session.execute_string(
            f"show streamlits like '{streamlit_name}'"
        )
        assert result.json == row_from_snowflake_session(expect)[0]

        result = runner.invoke_with_connection_json(
            ["streamlit", "describe", streamlit_name]
        )
        expect = snowflake_session.execute_string(
            f"describe streamlit {streamlit_name}"
        )
        assert result.json == row_from_snowflake_session(expect)[0]

        result = runner.invoke_with_connection_json(
            ["streamlit", "get-url", streamlit_name]
        )

        assert result.json["message"].endswith(
            f"/#/streamlit-apps/{test_database.upper()}.PUBLIC.{streamlit_name.upper()}"
        )

        result = runner.invoke_with_connection_json(
            ["streamlit", "share", streamlit_name, _new_streamlit_role]
        )
        assert contains_row_with(
            result.json,
            {"status": "Statement executed successfully."},
        )
        expect = snowflake_session.execute_string(
            f"use role {_new_streamlit_role}; show streamlits like '{streamlit_name}'; use role integration_tests;"
        )
        assert contains_row_with(
            rows_from_snowflake_session(expect)[1], {"name": streamlit_name.upper()}
        )

    result = runner.invoke_with_connection_json(
        ["object", "drop", "streamlit", streamlit_name]
    )
    assert result.json == {"status": f"{streamlit_name.upper()} successfully dropped."}
    expect = snowflake_session.execute_string(
        f"show streamlits like '{streamlit_name}'"
    )
    assert row_from_snowflake_session(expect) == []


@pytest.mark.integration
def test_fully_qualified_name(
    alter_snowflake_yml, test_database, project_directory, runner, snapshot
):
    default_schema = "PUBLIC"
    different_schema = "TOTALLY_DIFFERENT_SCHEMA"
    database = test_database.upper()
    assert (
        runner.invoke_with_connection(
            ["sql", "-q", f"create schema {database}.{different_schema}"]
        ).exit_code
        == 0
    )

    # test fully qualified name as name
    with project_directory("streamlit") as tmp_dir:
        streamlit_name = "streamlit_fqn"
        snowflake_yml = tmp_dir / "snowflake.yml"

        # FQN with "default" values
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path="streamlit.name",
            value=f"{database}.{default_schema}.{streamlit_name}",
        )
        result = runner.invoke_with_connection_json(["streamlit", "deploy"])
        assert result.exit_code == 0
        assert result.json == {
            "message": "Streamlit successfully deployed and available under "
            f"https://app.snowflake.com/SFENGINEERING/snowcli_it/#/streamlit-apps/{database}.{default_schema}.{streamlit_name.upper()}",
        }

        # FQN with different schema - should not conflict
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path="streamlit.name",
            value=f"{database}.{different_schema}.{streamlit_name}",
        )
        result = runner.invoke_with_connection_json(["streamlit", "deploy"])
        assert result.exit_code == 0
        assert result.json == {
            "message": "Streamlit successfully deployed and available under "
            f"https://app.snowflake.com/SFENGINEERING/snowcli_it/#/streamlit-apps/{database}.{different_schema}.{streamlit_name.upper()}",
        }

        # FQN with just schema provided - should require update
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path="streamlit.name",
            value=f"{different_schema}.{streamlit_name}",
        )
        result = runner.invoke_with_connection(
            ["streamlit", "deploy"], catch_exceptions=True
        )
        assert result.exit_code == 1
        # Same if name is not fqn but schema is specified
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path="streamlit.name",
            value=streamlit_name,
        )
        alter_snowflake_yml(
            snowflake_yml,
            parameter_path="streamlit.schema",
            value=different_schema,
        )
        result = runner.invoke_with_connection(
            ["streamlit", "deploy"], catch_exceptions=True
        )
        assert result.exit_code == 1

        # Should succeed with --replace flag
        result = runner.invoke_with_connection_json(
            ["streamlit", "deploy", "--replace"]
        )
        assert result.exit_code == 0
        assert result.json == {
            "message": "Streamlit successfully deployed and available under "
            f"https://app.snowflake.com/SFENGINEERING/snowcli_it/#/streamlit-apps/{database}.{different_schema}.{streamlit_name.upper()}",
        }


@pytest.fixture
def _new_streamlit_role(snowflake_session, test_database):
    role_name = f"snowcli_streamlit_role_{uuid.uuid4().hex}"
    result = snowflake_session.execute_string(
        f"set user = (select current_user()); "
        f"create role {role_name}; "
        f"grant all on database {test_database} to role {role_name};"
        f"grant usage on schema {test_database}.public to role {role_name}; "
        f"grant role {role_name} to user IDENTIFIER($USER)"
    )
    assert contains_row_with(
        row_from_snowflake_session(result),
        {"status": "Statement executed successfully."},
    )
    yield role_name
    result = snowflake_session.execute_string(f"drop role {role_name}")
    assert contains_row_with(
        row_from_snowflake_session(result),
        {"status": f"{role_name.upper()} successfully dropped."},
    )
