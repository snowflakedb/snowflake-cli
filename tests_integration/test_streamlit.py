import uuid
import pytest

from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import (
    row_from_snowflake_session,
    rows_from_snowflake_session,
    contains_row_with,
)


@pytest.mark.integration
def test_streamlit_create_and_deploy(
    runner,
    snowflake_session,
    test_database,
    _new_streamlit_role,
    test_root_path,
):
    streamlit_name = "test_streamlit_create_and_deploy_snowcli"
    streamlit_app_path = test_root_path / "test_files/streamlit.py"

    result = runner.invoke_integration(
        [
            "streamlit",
            "deploy",
            streamlit_name,
            "--file",
            streamlit_app_path,
            "--query-warehouse",
            snowflake_session.warehouse,
        ]
    )
    assert result.exit_code == 0

    result = runner.invoke_integration(["streamlit", "list"])
    expect = snowflake_session.execute_string(
        f"show streamlits like '{streamlit_name}'"
    )
    assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

    result = runner.invoke_integration(["streamlit", "describe", streamlit_name])
    expect = snowflake_session.execute_string(f"describe streamlit {streamlit_name}")
    assert contains_row_with(result.json[0], row_from_snowflake_session(expect)[0])
    expect = snowflake_session.execute_string(
        f"call system$generate_streamlit_url_from_name('{streamlit_name}')"
    )
    assert contains_row_with(result.json[1], row_from_snowflake_session(expect)[0])

    result = runner.invoke_integration(
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

    result = runner.invoke_integration(["streamlit", "drop", streamlit_name])
    assert contains_row_with(
        result.json,
        {"status": f"{streamlit_name.upper()} successfully dropped."},
    )
    expect = snowflake_session.execute_string(
        f"show streamlits like '{streamlit_name}'"
    )
    assert row_from_snowflake_session(expect) == []


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
