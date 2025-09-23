import os
from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    DCM_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    DCMProjectManager,
)
from snowflake.cli.api.constants import PatternMatchingType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import PathMapping

execute_queries = "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query"
TEST_STAGE = FQN.from_stage("@test_stage")
TEST_PROJECT = FQN.from_string("my_project")


@pytest.fixture
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.FQN.from_resource",
        return_value=FQN(
            database="MockDatabase",
            schema="MockSchema",
            name="DCM_TEST_PIPELINE_1757333281_OUTPUT_TMP_STAGE",
        ),
    ) as _fixture:
        yield _fixture


@mock.patch(execute_queries)
def test_create(mock_execute_query):
    project_identifier = FQN.from_string("project_mock_fqn")
    mgr = DCMProjectManager()
    mgr.create(project_identifier=project_identifier)

    mock_execute_query.assert_called_once_with(
        "CREATE DCM PROJECT IDENTIFIER('project_mock_fqn')"
    )


@mock.patch(execute_queries)
def test_execute_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
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
        project_identifier=TEST_PROJECT,
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
        project_identifier=TEST_PROJECT,
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

    mgr.execute(project_identifier=TEST_PROJECT, from_stage="@test_stage")

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_plan_project(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_plan_project_with_from_stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
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
    mgr.list_deployments(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="SHOW DEPLOYMENTS IN DCM PROJECT my_project"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize("if_exists", [True, False])
def test_drop_deployment(mock_execute_query, if_exists):
    mgr = DCMProjectManager()
    mgr.drop_deployment(
        project_identifier=TEST_PROJECT, deployment_name="v1", if_exists=if_exists
    )

    expected_query = "ALTER DCM PROJECT my_project DROP DEPLOYMENT"
    if if_exists:
        expected_query += " IF EXISTS"
    expected_query += ' "v1"'

    mock_execute_query.assert_called_once_with(query=expected_query)


@mock.patch(execute_queries)
def test_plan_project_with_output_path__stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        configuration="some_configuration",
        output_path="@output_stage/results",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage OUTPUT_PATH @output_stage/results"
    )


@mock.patch(execute_queries)
@mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
def test_plan_project_with_output_path__local_path(
    mock_create,
    mock_get_recursive,
    mock_execute_query,
    project_directory,
    mock_from_resource,
):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=True,
        configuration="some_configuration",
        output_path="output_path/results",
    )

    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage OUTPUT_PATH @{temp_stage_fqn}"
    )
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=str(temp_stage_fqn), dest_path=Path("output_path/results")
    )


@mock.patch(execute_queries)
def test_deploy_project_with_output_path(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.execute(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        dry_run=False,
        alias="v1",
        output_path="@output_stage",
    )

    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS \"v1\" FROM @test_stage"
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
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        alias=alias,
    )

    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS {expected_alias} FROM @test_stage"
    )


class TestSyncLocalFiles:
    def test_raises_when_manifest_file_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            with pytest.raises(
                CliError,
                match=f"{MANIFEST_FILE_NAME} was not found in directory",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_file_has_no_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            (project_dir / MANIFEST_FILE_NAME).touch()
            with pytest.raises(
                CliError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"definition": "v1"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_file_is_invalid(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"type": "spcs"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest file is defined for type spcs. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    @mock.patch("snowflake.cli._plugins.dcm.manager.sync_artifacts_with_stage")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_calls_sync_artifacts_with_stage(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        project_directory,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):

        with project_directory("dcm_project") as project_dir:
            DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

            mock_sync_artifacts_with_stage.assert_called_once()

            # due to Windows and inconsistent path resolution in unit tests,
            # we need to verify call arguments individually, with simplified path comparison
            call_args = mock_sync_artifacts_with_stage.call_args
            assert call_args.kwargs["stage_root"] == str(mock_from_resource())
            assert call_args.kwargs["artifacts"] == [
                PathMapping(src="definitions/my_query.sql"),
                PathMapping(src="manifest.yml", dest=None, processors=[]),
            ]
            assert call_args.kwargs["pattern_type"] == PatternMatchingType.REGEX
            assert call_args.kwargs["use_temporary_stage"] is True

            actual_project_root = call_args.kwargs["project_paths"].project_root
            expected_project_root = project_dir.resolve()
            assert actual_project_root.resolve() == expected_project_root.resolve()

    @mock.patch("snowflake.cli._plugins.dcm.manager.sync_artifacts_with_stage")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_with_source_directory(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        source_dir = tmp_path / "custom_source"
        source_dir.mkdir()

        manifest_content = {
            "type": "dcm_project",
            "include_definitions": ["definitions/custom_query.sql"],
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        # Create the definition file
        definitions_dir = source_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "custom_query.sql").write_text("SELECT 1;")

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT, source_directory=str(source_dir)
        )

        mock_sync_artifacts_with_stage.assert_called_once()
        call_args = mock_sync_artifacts_with_stage.call_args
        actual_project_root = call_args.kwargs["project_paths"].project_root
        assert actual_project_root.resolve() == source_dir.resolve()

    @mock.patch("snowflake.cli._plugins.dcm.manager.sync_artifacts_with_stage")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_with_relative_source_directory(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        source_dir = tmp_path / "relative_source"
        source_dir.mkdir()

        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump({"type": "dcm_project"}, f)

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            DCMProjectManager.sync_local_files(
                project_identifier=TEST_PROJECT,
                source_directory="relative_source",  # relative path
            )

            mock_sync_artifacts_with_stage.assert_called_once()
            call_args = mock_sync_artifacts_with_stage.call_args

            actual_project_root = call_args.kwargs["project_paths"].project_root
            assert actual_project_root.is_absolute()
            assert actual_project_root.resolve() == source_dir.resolve()
        finally:
            os.chdir(original_cwd)
