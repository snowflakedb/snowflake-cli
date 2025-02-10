from unittest import mock

from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli.api.identifiers import FQN

execute_queries = "snowflake.cli._plugins.project.commands.ProjectManager.execute_query"
TEST_STAGE = FQN.from_stage("@test_stage")
TEST_PROJECT = FQN.from_string("my_project")


@mock.patch(execute_queries)
def test_create_version(mock_execute_query, runner, project_directory):
    mgr = ProjectManager()
    mgr.create_version(project_name=TEST_PROJECT, stage_name=TEST_STAGE)

    mock_execute_query.assert_called_once_with(
        query="ALTER PROJECT my_project ADD VERSION FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_add_version(mock_execute_query, runner, project_directory):
    mgr = ProjectManager()
    mgr.add_version(
        project_name=TEST_PROJECT, from_stage="@stage_foo", alias="v1", comment="fancy"
    )

    mock_execute_query.assert_called_once_with(
        query="ALTER PROJECT my_project ADD VERSION IF NOT EXIST v1 FROM @stage_foo COMMENT = 'fancy'"
    )


@mock.patch(execute_queries)
def test_execute_project(mock_execute_query, runner, project_directory):
    mgr = ProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT, version="v42", variables=["key=value", "aaa=bbb"]
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE PROJECT IDENTIFIER('my_project') using (key=>value, aaa=>bbb) WITH VERSION v42"
    )


@mock.patch(execute_queries)
def test_validate_project(mock_execute_query, runner, project_directory):
    mgr = ProjectManager()
    mgr.execute(project_name=TEST_PROJECT, version="v42", dry_run=True)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE PROJECT IDENTIFIER('my_project') WITH VERSION v42 DRY_RUN=TRUE"
    )
