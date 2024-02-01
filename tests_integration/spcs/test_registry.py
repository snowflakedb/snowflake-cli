import pytest
from snowflake.connector.cursor import DictCursor

from tests_integration.testing_utils.naming_utils import ObjectNameProvider


@pytest.mark.integration
def test_token(runner):
    result = runner.invoke_with_connection_json(["spcs", "image-registry", "token"])

    assert result.exit_code == 0
    assert result.json
    assert "token" in result.json
    assert result.json["token"]
    assert "expires_in" in result.json
    assert result.json["expires_in"]


@pytest.mark.integration
def test_get_registry_url(test_database, test_role, runner, snowflake_session):
    # newly created role should have no access to image repositories and should not be able to get registry URL
    test_repo = ObjectNameProvider("test_repo").create_and_get_next_object_name()
    snowflake_session.execute_string(f"create image repository {test_repo}")

    fail_result = runner.invoke_with_connection(
        ["spcs", "image-registry", "url", "--role", test_role]
    )
    assert fail_result.exit_code == 1, fail_result.output
    assert "Current role cannot view any image repositories." in fail_result.output

    # role should be able to get registry URL once granted read access to an image repository
    repo_list_cursor = snowflake_session.execute_string(
        "show image repositories", cursor_class=DictCursor
    )
    expected_repo_url = repo_list_cursor[0].fetchone()["repository_url"]
    expected_registry_url = "/".join(expected_repo_url.split("/")[:-3])
    snowflake_session.execute_string(
        f"grant usage on database {snowflake_session.database} to role {test_role};"
        f"grant usage on schema {snowflake_session.schema} to role {test_role};"
        f"grant read on image repository {test_repo} to role {test_role};"
    )
    success_result = runner.invoke_with_connection(
        ["spcs", "image-registry", "url", "--role", test_role]
    )
    assert success_result.exit_code == 0, success_result.output
    assert success_result.output.strip() == expected_registry_url
