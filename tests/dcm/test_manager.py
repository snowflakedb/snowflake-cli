from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli.api.identifiers import FQN

execute_queries = "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query"
TEST_STAGE = FQN.from_stage("@test_stage")
TEST_PROJECT = FQN.from_string("my_project")


@mock.patch(execute_queries)
def test_create(mock_execute_query):
    project_mock = mock.MagicMock(
        fqn=FQN.from_string("project_mock_fqn"), stage="mock_stage_name"
    )
    mgr = DCMProjectManager()
    mgr.create(project=project_mock)

    mock_execute_query.assert_called_once_with(
        "CREATE DCM PROJECT IDENTIFIER('project_mock_fqn')"
    )


@mock.patch(execute_queries)
def test_execute_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_execute_project_with_from_stage(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@my_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @my_stage"
    )


@mock.patch(execute_queries)
def test_execute_project_with_from_stage_without_prefix(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="my_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @my_stage"
    )


@mock.patch(execute_queries)
def test_execute_project_with_default_deployment(mock_execute_query, project_directory):
    mgr = DCMProjectManager()

    mgr.execute(project_name=TEST_PROJECT, from_stage="@test_stage")

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_validate_project(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_validate_project_with_from_stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@my_stage",
        dry_run=True,
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration"
        " FROM @my_stage"
    )


@mock.patch(execute_queries)
def test_list_deployments(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.list_deployments(project_name=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="SHOW DEPLOYMENTS IN DCM PROJECT my_project"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize("if_exists", [True, False])
def test_drop_deployment(mock_execute_query, if_exists):
    mgr = DCMProjectManager()
    mgr.drop_deployment(
        project_name=TEST_PROJECT, deployment_name="v1", if_exists=if_exists
    )

    expected_query = "ALTER DCM PROJECT my_project DROP DEPLOYMENT"
    if if_exists:
        expected_query += " IF EXISTS"
    expected_query += ' "v1"'

    mock_execute_query.assert_called_once_with(query=expected_query)


@mock.patch(execute_queries)
def test_validate_project_with_output_path(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        configuration="some_configuration",
        output_path="@output_stage/results",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage OUTPUT_PATH @output_stage/results"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize(
    "output_stage_name", ["@output_stage/path", "output_stage/path"]
)
def test_validate_project_with_output_path_different_formats(
    mock_execute_query, project_directory, output_stage_name
):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        output_path=output_stage_name,
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN FROM @test_stage OUTPUT_PATH @output_stage/path"
    )


@mock.patch(execute_queries)
def test_deploy_project_with_output_path(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=False,
        alias="v1",
        output_path="@output_stage",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS \"v1\" FROM @test_stage OUTPUT_PATH @output_stage"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize(
    "alias,expected_alias",
    [
        ("test-1", '"test-1"'),
        ("my alias", '"my alias"'),
        ("v1.0", '"v1.0"'),
        ("test_alias", '"test_alias"'),
        ("v1", '"v1"'),
    ],
)
def test_deploy_project_with_alias_special_characters(
    mock_execute_query, alias, expected_alias
):
    mgr = DCMProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT,
        from_stage="@test_stage",
        alias=alias,
    )

    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS {expected_alias} FROM @test_stage"
    )
