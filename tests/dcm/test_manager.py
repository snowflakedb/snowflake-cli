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

import os
from pathlib import Path
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    SOURCES_FOLDER,
    DCMProjectManager,
)
from snowflake.cli._plugins.dcm.models import MANIFEST_FILE_NAME
from snowflake.cli.api.constants import PatternMatchingType
from snowflake.cli.api.identifiers import FQN

execute_queries = "snowflake.cli._plugins.dcm.manager.DCMProjectManager.execute_query"
TEST_STAGE = FQN.from_stage("@test_stage")
TEST_PROJECT = FQN.from_string("my_project")


@pytest.fixture
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dcm.manager.FQN.from_resource",
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
def test_analyze_project_basic(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_configuration(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING CONFIGURATION some_configuration FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_variables(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        variables=["key=value", "aaa=bbb"],
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING"
        " (key=>value, aaa=>bbb) FROM @test_stage"
    )


@mock.patch(execute_queries)
def test_analyze_project_with_configuration_and_variables(mock_execute_query):
    mgr = DCMProjectManager()
    mgr.raw_analyze(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        variables=["key=value", "aaa=bbb"],
    )

    mock_execute_query.assert_called_once_with(
        query="EXECUTE DCM PROJECT IDENTIFIER('my_project') ANALYZE USING CONFIGURATION some_configuration"
        " (key=>value, aaa=>bbb) FROM @test_stage"
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
def test_plan_project_default_no_download(mock_execute_query, project_directory):
    mgr = DCMProjectManager()

    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "OUTPUT_PATH" not in query


@mock.patch("snowflake.cli._plugins.dcm.manager.FQN.from_resource")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
def test_plan_project_with_save_output(
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    project_directory,
):
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
        save_output=True,
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "OUTPUT_PATH" in query
    mock_get_recursive.assert_called_once()


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
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
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
            save_output=True,
        )

    # But the output should still be downloaded before exception is reraised
    temp_stage_fqn = mock_from_resource()
    mock_execute_query.assert_called_once()
    mock_create.assert_called_once_with(temp_stage_fqn, temporary=True)
    mock_get_recursive.assert_called_once_with(
        stage_path=f"@{str(temp_stage_fqn)}/outputs",
        dest_path=Path("out"),
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


class TestSyncLocalFiles:
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

            artifacts = call_args.kwargs["artifacts"]
            artifact_srcs = {a.src for a in artifacts}
            assert MANIFEST_FILE_NAME in artifact_srcs
            assert any(SOURCES_FOLDER in src for src in artifact_srcs)

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
            "manifest_version": 2,
            "type": "dcm_project",
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()
        (sources_dir / "custom_query.sql").touch()

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT, source_directory=str(source_dir)
        )

        mock_sync_artifacts_with_stage.assert_called_once()
        call_args = mock_sync_artifacts_with_stage.call_args
        actual_project_root = call_args.kwargs["project_paths"].project_root
        assert actual_project_root.resolve() == source_dir.resolve()

        artifacts = call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]
        assert MANIFEST_FILE_NAME in artifact_srcs
        assert any(
            SOURCES_FOLDER in src and "custom_query.sql" in src for src in artifact_srcs
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
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)

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
    def test_sync_local_files_includes_all_files_in_sources(
        self,
        _mock_create_stage,
        mock_sync_artifacts_with_stage,
        tmp_path,
        mock_connect,
        mock_cursor,
        mock_from_resource,
    ):
        source_dir = tmp_path / "project_with_sources"
        source_dir.mkdir()

        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump({"manifest_version": 2, "type": "dcm_project"}, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()

        definitions_dir = sources_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "table.sql").touch()

        macros_dir = sources_dir / "macros"
        macros_dir.mkdir()
        (macros_dir / "helpers.sql").touch()
        (macros_dir / "utils.jinja").touch()

        (sources_dir / "dbt_project.yml").touch()

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
        assert any("dbt_project.yml" in src for src in artifact_srcs)
