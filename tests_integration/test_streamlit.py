import uuid
from pathlib import Path
from textwrap import dedent

import pytest
import os

from tests_integration.conftest import project_directory
from tests_integration.testing_utils.working_directory_utils import (
    temporary_working_directory,
)
from tests_integration.snowflake_connector import test_database, snowflake_session
from tests_integration.test_utils import (
    row_from_snowflake_session,
    rows_from_snowflake_session,
    contains_row_with,
)


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
        result = runner.invoke_integration(["streamlit", "deploy"])
        assert result.exit_code == 0

        result = runner.invoke_integration(["streamlit", "list"])
        expect = snowflake_session.execute_string(
            f"show streamlits like '{streamlit_name}'"
        )
        assert contains_row_with(result.json, row_from_snowflake_session(expect)[0])

        result = runner.invoke_integration(["streamlit", "describe", streamlit_name])
        expect = snowflake_session.execute_string(
            f"describe streamlit {streamlit_name}"
        )
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


def _create_project_definition(
    temporary_working_directory: Path, streamlit_name: str, streamlit_app_path: str
) -> None:
    file_path = os.path.join(temporary_working_directory, "snowflake.yml")
    with open(file_path, "w") as new_file:
        new_file.write(
            dedent(
                f"""
        definition_version: 1
        streamlits:
          - name: {streamlit_name}
            file: {streamlit_app_path}
        """
            )
        )
