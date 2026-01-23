import os
from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    DCM_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    REQUIRED_MANIFEST_VERSION,
    DCMManifest,
    DCMProjectManager,
    DCMTemplating,
)
from snowflake.cli.api.constants import PatternMatchingType
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN

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
def test_deploy_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.deploy(
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
def test_deploy_project_with_skip_plan(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.deploy(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
        configuration="some_configuration",
        skip_plan=True,
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage SKIP PLAN"
    )


@mock.patch(execute_queries)
def test_deploy_project_with_from_stage(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.deploy(
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
def test_deploy_project_with_from_stage_without_prefix(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.deploy(
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
def test_deploy_project_with_default_deployment(mock_execute_query, project_directory):
    mgr = DCMProjectManager()

    mgr.deploy(project_identifier=TEST_PROJECT, from_stage="@test_stage")

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_plan_project(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_plan_project_with_from_stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
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
def test_preview_project_basic(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.preview(
        project_identifier=TEST_PROJECT,
        object_identifier=FQN.from_string("my_table"),
        from_stage="@test_stage",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') PREVIEW IDENTIFIER('my_table') FROM @test_stage"
    )


@mock.patch(execute_queries)
@pytest.mark.parametrize(
    "configuration,variables,limit,expected_suffix",
    [
        (
            "dev",
            ["key=value"],
            10,
            " USING CONFIGURATION dev (key=>value) FROM @test_stage LIMIT 10",
        ),
        (
            "prod",
            None,
            None,
            " USING CONFIGURATION prod FROM @test_stage",
        ),
        (
            None,
            ["var1=val1", "var2=val2"],
            5,
            " USING (var1=>val1, var2=>val2) FROM @test_stage LIMIT 5",
        ),
        (
            None,
            None,
            100,
            " FROM @test_stage LIMIT 100",
        ),
    ],
)
def test_preview_project_with_various_options(
    mock_execute_query, configuration, variables, limit, expected_suffix
):
    mgr = DCMProjectManager()
    mgr.preview(
        project_identifier=TEST_PROJECT,
        object_identifier=FQN.from_string("my_view"),
        from_stage="@test_stage",
        configuration=configuration,
        variables=variables,
        limit=limit,
    )

    expected_query = (
        f"EXECUTE DCM PROJECT IDENTIFIER('my_project') PREVIEW IDENTIFIER('my_view')"
        + expected_suffix
    )
    mock_execute_query.assert_called_once_with(query=expected_query)


@mock.patch(execute_queries)
def test_refresh_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.refresh(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') REFRESH ALL"
    )


@mock.patch(execute_queries)
def test_test_project(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.test(project_identifier=TEST_PROJECT)

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') TEST ALL"
    )


@mock.patch(execute_queries)
def test_plan_project_with_output_path__stage(mock_execute_query, project_directory):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
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
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        output_path="output_path/results",
    )

    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN USING CONFIGURATION some_configuration FROM @test_stage OUTPUT_PATH @{temp_stage_fqn}/outputs"
    )
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("output_path/results"),
    )


@mock.patch(execute_queries)
@mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
def test_plan_project_with_output_path__exception_handling(
    mock_create,
    mock_get_recursive,
    mock_execute_query,
    project_directory,
    mock_from_resource,
):
    mock_execute_query.side_effect = Exception("Query execution failed")

    mgr = DCMProjectManager()

    with pytest.raises(Exception, match="Query execution failed"):
        mgr.plan(
            project_identifier=TEST_PROJECT,
            from_stage="@test_stage",
            configuration="some_configuration",
            output_path="output_path/results",
        )

    # But the output should still be downloaded before exception is reraised
    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once()
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("output_path/results"),
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
    mgr.deploy(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        alias=alias,
    )

    mock_execute_query.assert_called_once_with(
        query=f"EXECUTE DCM PROJECT IDENTIFIER('my_project') DEPLOY AS {expected_alias} FROM @test_stage"
    )


class TestDCMManifest:
    def test_manifest_from_dict_minimal(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == "2.0"
        assert manifest.project_type == "dcm_project"
        assert manifest.templating.global_variables == {}
        assert manifest.templating.configurations == {}

    def test_manifest_from_dict_with_templating(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "global_variables": {"db_name": "shared_db", "retry_count": 3},
                "configurations": {
                    "dev": {"wh_size": "XSMALL", "suffix": "_dev"},
                    "prod": {"wh_size": "LARGE", "suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == "2.0"
        assert manifest.project_type == "dcm_project"
        assert manifest.templating.global_variables == {
            "db_name": "shared_db",
            "retry_count": 3,
        }
        assert manifest.templating.configurations == {
            "dev": {"wh_size": "XSMALL", "suffix": "_dev"},
            "prod": {"wh_size": "LARGE", "suffix": ""},
        }

    def test_manifest_get_configuration_names(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "staging": {"suffix": "_stg"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        config_names = manifest.get_configuration_names()
        assert set(config_names) == {"dev", "staging", "prod"}

    def test_manifest_validate_success(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_validate_missing_type(self):
        data = {"manifest_version": "2.0", "type": ""}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(CliError, match="Manifest file type is undefined"):
            manifest.validate()

    def test_manifest_validate_wrong_type(self):
        data = {"manifest_version": "2.0", "type": "wrong_type"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            CliError, match="Manifest file is defined for type wrong_type"
        ):
            manifest.validate()

    def test_manifest_validate_wrong_version(self):
        data = {"manifest_version": "1.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(CliError, match="Manifest version '1.0' is not supported"):
            manifest.validate()


class TestDCMTemplating:
    def test_templating_from_dict_none(self):
        templating = DCMTemplating.from_dict(None)

        assert templating.global_variables == {}
        assert templating.configurations == {}

    def test_templating_from_dict_empty(self):
        templating = DCMTemplating.from_dict({})

        assert templating.global_variables == {}
        assert templating.configurations == {}

    def test_templating_from_dict_with_data(self):
        data = {
            "global_variables": {"key": "value"},
            "configurations": {"dev": {"suffix": "_dev"}},
        }
        templating = DCMTemplating.from_dict(data)

        assert templating.global_variables == {"key": "value"}
        assert templating.configurations == {"dev": {"suffix": "_dev"}}


class TestSyncLocalFiles:
    def test_raises_when_manifest_file_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            with pytest.raises(
                CliError,
                match=f"{MANIFEST_FILE_NAME} was not found in directory",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_file_is_empty(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            (project_dir / MANIFEST_FILE_NAME).touch()
            with pytest.raises(
                CliError,
                match="Manifest file is empty or invalid",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_file_has_no_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "definition": "v1"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_file_has_wrong_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "type": "spcs"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest file is defined for type spcs. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_version_is_invalid(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "1", "type": "dcm_project"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest version '1' is not supported. Expected {REQUIRED_MANIFEST_VERSION}",
            ):
                DCMProjectManager.sync_local_files(project_identifier=TEST_PROJECT)

    def test_raises_when_manifest_version_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"type": "dcm_project"}, f)
            with pytest.raises(
                CliError,
                match=f"Manifest version '' is not supported. Expected {REQUIRED_MANIFEST_VERSION}",
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

            call_args = mock_sync_artifacts_with_stage.call_args
            assert call_args.kwargs["stage_root"] == str(mock_from_resource())

            # V2 manifest uses convention-based folders - all .sql files in definitions/
            artifacts = call_args.kwargs["artifacts"]
            artifact_srcs = {a.src for a in artifacts}
            assert MANIFEST_FILE_NAME in artifact_srcs
            # Check that definitions folder files are included
            assert any("definitions" in src for src in artifact_srcs)

            assert call_args.kwargs["pattern_type"] == PatternMatchingType.GLOB
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
            "manifest_version": "2.0",
            "type": "dcm_project",
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        # Create the definition file in definitions/ folder
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

        # Verify artifacts include the definitions file
        artifacts = call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]
        assert MANIFEST_FILE_NAME in artifact_srcs
        assert any(
            "definitions" in src and "custom_query.sql" in src for src in artifact_srcs
        )

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
            yaml.dump({"manifest_version": "2.0", "type": "dcm_project"}, f)

        original_cwd = os.getcwd()
        try:
            os.chdir(tmp_path)

            DCMProjectManager.sync_local_files(
                project_identifier=TEST_PROJECT,
                source_directory="relative_source",
            )

            mock_sync_artifacts_with_stage.assert_called_once()
            call_args = mock_sync_artifacts_with_stage.call_args

            actual_project_root = call_args.kwargs["project_paths"].project_root
            assert actual_project_root.is_absolute()
            assert actual_project_root.resolve() == source_dir.resolve()
        finally:
            os.chdir(original_cwd)

    @mock.patch("snowflake.cli._plugins.dcm.manager.sync_artifacts_with_stage")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_includes_macros_folder(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        source_dir = tmp_path / "project_with_macros"
        source_dir.mkdir()

        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump({"manifest_version": "2.0", "type": "dcm_project"}, f)

        # Create definitions folder with SQL files
        definitions_dir = source_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "table.sql").write_text("SELECT 1;")

        # Create macros folder with macro files
        macros_dir = source_dir / "macros"
        macros_dir.mkdir()
        (macros_dir / "helpers.sql").write_text("-- macro")
        (macros_dir / "utils.jinja").write_text("{% macro test() %}{% endmacro %}")

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT, source_directory=str(source_dir)
        )

        mock_sync_artifacts_with_stage.assert_called_once()
        call_args = mock_sync_artifacts_with_stage.call_args

        artifacts = call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]

        assert MANIFEST_FILE_NAME in artifact_srcs
        assert any("definitions" in src and "table.sql" in src for src in artifact_srcs)
        assert any("macros" in src and "helpers.sql" in src for src in artifact_srcs)
        assert any("macros" in src and "utils.jinja" in src for src in artifact_srcs)

    @mock.patch("snowflake.cli._plugins.dcm.manager.sync_artifacts_with_stage")
    @mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
    def test_sync_local_files_with_templating_section(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        source_dir = tmp_path / "project_with_templating"
        source_dir.mkdir()

        manifest_content = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "global_variables": {"db_name": "shared_db"},
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        definitions_dir = source_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "table.sql").write_text("SELECT 1;")

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT, source_directory=str(source_dir)
        )

        mock_sync_artifacts_with_stage.assert_called_once()
        call_args = mock_sync_artifacts_with_stage.call_args
        artifacts = call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]
        assert MANIFEST_FILE_NAME in artifact_srcs
