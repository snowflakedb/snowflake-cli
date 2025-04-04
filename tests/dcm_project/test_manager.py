from unittest import mock

import pytest
from snowflake.cli._plugins.project.manager import ProjectManager
from snowflake.cli.api.identifiers import FQN

execute_queries = "snowflake.cli._plugins.project.manager.ProjectManager.execute_query"
sync_artifacts_with_stage = (
    "snowflake.cli._plugins.project.manager.sync_artifacts_with_stage"
)
projects_paths = "snowflake.cli._plugins.project.manager.ProjectPaths"
TEST_STAGE = FQN.from_stage("@test_stage")
TEST_PROJECT = FQN.from_string("my_project")


@mock.patch(execute_queries)
@mock.patch(sync_artifacts_with_stage)
@mock.patch(projects_paths)
@pytest.mark.parametrize(
    "from_stage,prune", [("stage_foo", False), (None, False), (None, True)]
)
def test_add_version(
    mock_project_paths, mock_sync_artifacts, mock_execute_query, from_stage, prune
):
    project_mock = mock.MagicMock(
        fqn=FQN.from_string("project_mock_fqn"),
        stage="stage_from_project",
        artifacts=["project_artifacts"],
    )
    mock_project_paths.return_value = "mock_project_paths"

    mgr = ProjectManager()
    mgr.add_version(project=project_mock, prune=prune, from_stage=from_stage)

    if from_stage:
        expected_stage = from_stage
        mock_sync_artifacts.assert_not_called()
    else:
        expected_stage = "stage_from_project"
        mock_sync_artifacts.assert_called_once_with(
            project_paths="mock_project_paths",
            stage_root=project_mock.stage,
            artifacts=project_mock.artifacts,
            prune=prune,
        )

    mock_execute_query.assert_called_once_with(
        query=f"ALTER PROJECT project_mock_fqn ADD VERSION FROM @{expected_stage}"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize("stage_name", ["@stage_foo", "stage_foo"])
def test_create_version(mock_execute_query, stage_name):
    mgr = ProjectManager()
    mgr._create_version(  # noqa: SLF001
        project_name=TEST_PROJECT, from_stage=stage_name, alias="v1", comment="fancy"
    )
    mock_execute_query.assert_called_once_with(
        query=f"ALTER PROJECT my_project ADD VERSION IF NOT EXISTS \"v1\" FROM @stage_foo COMMENT = 'fancy'"
    )


@mock.patch(execute_queries)
def test_create_version_no_alias(mock_execute_query):
    mgr = ProjectManager()
    mgr._create_version(  # noqa: SLF001
        project_name=TEST_PROJECT, from_stage="@stage_foo"
    )
    mock_execute_query.assert_called_once_with(
        query="ALTER PROJECT my_project ADD VERSION FROM @stage_foo"
    )


@mock.patch(execute_queries)
@mock.patch(sync_artifacts_with_stage)
@pytest.mark.parametrize("initialize_version", [True, False])
def test_create(mock_sync_artifacts, mock_execute_query, initialize_version):
    project_mock = mock.MagicMock(
        fqn=FQN.from_string("project_mock_fqn"), stage="mock_stage_name"
    )
    mgr = ProjectManager()
    mgr.create(
        project=project_mock, initialize_version_from_local_files=initialize_version
    )

    if initialize_version:
        mock_sync_artifacts.assert_called_once()
        assert mock_execute_query.mock_calls == [
            mock.call("CREATE PROJECT IDENTIFIER('project_mock_fqn')"),
            mock.call(
                query="ALTER PROJECT project_mock_fqn ADD VERSION FROM @mock_stage_name"
            ),
        ]
    else:
        mock_execute_query.assert_called_once_with(
            "CREATE PROJECT IDENTIFIER('project_mock_fqn')"
        )
        mock_sync_artifacts.assert_not_called()


@mock.patch(execute_queries)
def test_execute_project(mock_execute_query):
    mgr = ProjectManager()
    mgr.execute(
        project_name=TEST_PROJECT, version="v42", variables=["key=value", "aaa=bbb"]
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE PROJECT IDENTIFIER('my_project') using (key=>value, aaa=>bbb) WITH VERSION v42"
    )


@mock.patch(execute_queries)
def test_execute_project_with_default_version(mock_execute_query, project_directory):
    mgr = ProjectManager()

    mgr.execute(project_name=TEST_PROJECT, version=None)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE PROJECT IDENTIFIER('my_project')"
    )


@mock.patch(execute_queries)
def test_validate_project(mock_execute_query, project_directory):
    mgr = ProjectManager()
    mgr.execute(project_name=TEST_PROJECT, version="v42", dry_run=True)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE PROJECT IDENTIFIER('my_project') WITH VERSION v42 DRY_RUN=TRUE"
    )


@mock.patch(execute_queries)
def test_list_versions(mock_execute_query):
    mgr = ProjectManager()
    mgr.list_versions(project_name=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="SHOW VERSIONS IN PROJECT my_project"
    )
