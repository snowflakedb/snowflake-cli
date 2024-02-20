import os
import uuid

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

        result = runner.invoke_with_connection_json(["object", "list", "streamlit"])
        assert_that_result_is_successful(result)

        expect = snowflake_session.execute_string(
            f"show streamlits like '{streamlit_name}'"
        )
        assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

        result = runner.invoke_with_connection_json(
            ["object", "describe", "streamlit", streamlit_name]
        )
        expect = snowflake_session.execute_string(
            f"describe streamlit {streamlit_name}"
        )
        assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

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
    assert contains_row_with(
        result.json,
        {"status": f"{streamlit_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(
        f"show streamlits like '{streamlit_name}'"
    )
    assert row_from_snowflake_session(expect) == []


@pytest.fixture
def _create_schemas(runner, test_database):
    custom_schema = "custom_schema"
    different_schema = "totally_different_schema"
    for schema in [custom_schema, different_schema]:
        runner.invoke_with_connection(["sql", "-q", f"create schema {schema}"])
    yield
    for schema in [custom_schema, different_schema]:
        runner.invoke_with_connection(["object", "drop", "schema", schema])


@pytest.mark.integration
def test_streamlit_deploy_with_defined_stage(
    _create_schemas, runner, multiple_project_directories
):
    custom_schema = "custom_schema"
    different_schema = "totally_different_schema"

    def invoke_from_workdir(path, command):
        os.chdir(path)
        result = runner.invoke_with_connection(command)
        assert_that_result_is_successful(result)
        return result

    def _assert_streamlit_exists(schema=None):
        cmd = ["object", "list", "streamlit"]
        if schema is not None:
            cmd += ["--schema", schema]
        result = runner.invoke_with_connection_json(cmd)
        assert_that_result_is_successful(result)
        assert len(result.json) == 1
        assert result.json[0]["schema_name"] == schema.upper() if schema else "PUBLIC"

    with multiple_project_directories("streamlit", "streamlit_with_defined_schema") as (
        streamlit_public,
        streamlit_custom,
    ):
        # streamlits from different schemas should not conflict
        invoke_from_workdir(streamlit_public, ["streamlit", "deploy"])
        invoke_from_workdir(
            streamlit_public, ["streamlit", "deploy", "--schema", different_schema]
        )
        invoke_from_workdir(streamlit_custom, ["streamlit", "deploy"])

        # streamlits from different schemas are independent - list shows only one
        _assert_streamlit_exists()
        _assert_streamlit_exists(different_schema)
        _assert_streamlit_exists(custom_schema)


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

        result = runner.invoke_with_connection_json(["object", "list", "streamlit"])
        assert_that_result_is_successful(result)

        expect = snowflake_session.execute_string(
            f"show streamlits like '{streamlit_name}'"
        )
        assert result.json == row_from_snowflake_session(expect)[0]

        result = runner.invoke_with_connection_json(
            ["object", "describe", "streamlit", streamlit_name]
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
def test_streamlit_is_visible_in_anaconda_channel():
    from requirements.requirement import Requirement
    from snowflake.cli.plugins.snowpark.package_utils import parse_anaconda_packages

    streamlit = Requirement.parse_line("streamlit")

    result = parse_anaconda_packages([streamlit])

    assert streamlit in result.snowflake


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
