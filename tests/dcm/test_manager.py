import os
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dcm.manager import (
    SOURCES_FOLDER,
    DCMProjectManager,
)
from snowflake.cli._plugins.dcm.manifest import (
    DCM_PROJECT_TYPE,
    MANIFEST_FILE_NAME,
    DCMManifest,
    DCMTarget,
    DCMTemplating,
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli.api.constants import PatternMatchingType
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

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
def test_plan_project_default_no_download(mock_execute_query, project_directory):
    """Test plan without save_output (default) - no OUTPUT_PATH in query."""
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@test_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "USING CONFIGURATION some_configuration" in query
    assert "FROM @test_stage" in query
    assert "OUTPUT_PATH" not in query


@mock.patch("snowflake.cli._plugins.dcm.manager.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.manager.FQN.from_resource")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.manager.StageManager.create")
@mock.patch(execute_queries)
def test_plan_project_with_save_output(
    mock_execute_query,
    mock_create,
    mock_get_recursive,
    mock_from_resource,
    mock_cli_console,
    project_directory,
):
    """Test plan with save_output=True - files downloaded to out/."""
    mock_from_resource.return_value = FQN.from_string("TMP_STAGE")
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
    assert "USING CONFIGURATION some_configuration" in query
    assert "FROM @test_stage" in query
    assert "OUTPUT_PATH" in query
    mock_get_recursive.assert_called_once()


@mock.patch(execute_queries)
def test_plan_project_with_from_stage(mock_execute_query, project_directory):
    """Test plan with different from_stage - default behavior without OUTPUT_PATH."""
    mgr = DCMProjectManager()
    mgr.plan(
        project_identifier=TEST_PROJECT,
        from_stage="@my_stage",
        configuration="some_configuration",
    )

    mock_execute_query.assert_called_once()
    query = mock_execute_query.call_args.kwargs["query"]
    assert "EXECUTE DCM PROJECT IDENTIFIER('my_project') PLAN" in query
    assert "USING CONFIGURATION some_configuration" in query
    assert "FROM @my_stage" in query
    assert "OUTPUT_PATH" not in query


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
        assert manifest.default_target is None
        assert manifest.targets == {}
        assert manifest.templating.defaults == {}
        assert manifest.templating.configurations == {}

    def test_manifest_from_dict_with_targets(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {
                    "project_name": "DB.SCHEMA.PROJECT_DEV",
                    "templating_config": "dev",
                },
                "PROD": {
                    "project_name": "DB.SCHEMA.PROJECT_PROD",
                    "templating_config": "prod",
                },
            },
            "templating": {
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        assert manifest.default_target == "DEV"
        assert len(manifest.targets) == 2
        assert manifest.targets["DEV"].project_name == "DB.SCHEMA.PROJECT_DEV"
        assert manifest.targets["DEV"].templating_config == "dev"
        assert manifest.targets["PROD"].project_name == "DB.SCHEMA.PROJECT_PROD"
        assert manifest.targets["PROD"].templating_config == "prod"

    def test_manifest_from_dict_with_templating(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "templating": {
                "defaults": {"db_name": "shared_db", "retry_count": 3},
                "configurations": {
                    "dev": {"wh_size": "XSMALL", "suffix": "_dev"},
                    "prod": {"wh_size": "LARGE", "suffix": ""},
                },
            },
        }
        manifest = DCMManifest.from_dict(data)

        assert manifest.manifest_version == "2.0"
        assert manifest.project_type == "dcm_project"
        assert manifest.templating.defaults == {
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

    def test_manifest_get_target_names(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target_names = manifest.get_target_names()
        assert set(target_names) == {"DEV", "PROD"}

    def test_manifest_get_target(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "DB.SCHEMA.PROJECT_DEV"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_target("DEV")
        assert target.project_name == "DB.SCHEMA.PROJECT_DEV"

    def test_manifest_get_target_not_found(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {},
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError, match="Target 'UNKNOWN' not found in manifest"
        ):
            manifest.get_target("UNKNOWN")

    def test_manifest_get_effective_target_explicit(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target("PROD")
        assert target.project_name == "P2"

    def test_manifest_get_effective_target_uses_default(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target()
        assert target.project_name == "P1"

    def test_manifest_get_effective_target_no_default(self):
        """When multiple targets exist and no default_target is defined, should raise error."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
                "PROD": {"project_name": "P2"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            ManifestConfigurationError,
            match="No target specified and no default_target defined",
        ):
            manifest.get_effective_target()

    def test_manifest_single_target_auto_default(self):
        """When only one target exists and no default_target is defined, it should be auto-selected."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {
                "DEV": {"project_name": "P1"},
            },
        }
        manifest = DCMManifest.from_dict(data)

        target = manifest.get_effective_target()
        assert target.project_name == "P1"

    def test_manifest_validate_success(self):
        data = {"manifest_version": "2.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_validate_with_targets_success(self):
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "default_target": "DEV",
            "targets": {
                "DEV": {"project_name": "P1", "templating_config": "dev"},
            },
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_validate_missing_type(self):
        data = {"manifest_version": "2.0", "type": ""}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError, match="Manifest file type is undefined"
        ):
            manifest.validate()

    def test_manifest_validate_wrong_type(self):
        data = {"manifest_version": "2.0", "type": "wrong_type"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError, match="Manifest file is defined for type wrong_type"
        ):
            manifest.validate()

    def test_manifest_validate_wrong_version(self):
        data = {"manifest_version": "1.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError,
            match="Manifest version '1.0' is not supported.*>= 2.0 and < 3.0",
        ):
            manifest.validate()

    def test_manifest_validate_version_3_not_supported(self):
        data = {"manifest_version": "3.0", "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)

        with pytest.raises(
            InvalidManifestError,
            match="Manifest version '3.0' is not supported.*>= 2.0 and < 3.0",
        ):
            manifest.validate()

    @pytest.mark.parametrize("version", ["2", "2.0", "2.1", "2.5", "2.99"])
    def test_manifest_validate_valid_versions(self, version):
        data = {"manifest_version": version, "type": "dcm_project"}
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

    def test_manifest_get_target_unknown_configuration(self):
        """Configuration validation happens when getting target, not during validate()."""
        data = {
            "manifest_version": "2.0",
            "type": "dcm_project",
            "targets": {"DEV": {"project_name": "P1", "templating_config": "unknown"}},
            "templating": {"configurations": {"dev": {}}},
        }
        manifest = DCMManifest.from_dict(data)
        manifest.validate()

        with pytest.raises(
            ManifestConfigurationError,
            match="Target 'DEV' references unknown configuration 'unknown'",
        ):
            manifest.get_target("DEV")


class TestDCMTemplating:
    def test_templating_from_dict_none(self):
        templating = DCMTemplating.from_dict(None)

        assert templating.defaults == {}
        assert templating.configurations == {}

    def test_templating_from_dict_empty(self):
        templating = DCMTemplating.from_dict({})

        assert templating.defaults == {}
        assert templating.configurations == {}

    def test_templating_from_dict_with_data(self):
        data = {
            "defaults": {"key": "value"},
            "configurations": {"dev": {"suffix": "_dev"}},
        }
        templating = DCMTemplating.from_dict(data)

        assert templating.defaults == {"key": "value"}
        assert templating.configurations == {"dev": {"suffix": "_dev"}}


class TestDCMTarget:
    def test_target_from_dict_minimal(self):
        data = {"project_name": "DB.SCHEMA.MY_PROJECT"}
        target = DCMTarget.from_dict(data)

        assert target.project_name == "DB.SCHEMA.MY_PROJECT"
        assert target.templating_config is None

    def test_target_from_dict_full(self):
        data = {
            "project_name": "DB.SCHEMA.MY_PROJECT",
            "templating_config": "dev",
        }
        target = DCMTarget.from_dict(data)

        assert target.project_name == "DB.SCHEMA.MY_PROJECT"
        assert target.templating_config == "dev"


class TestLoadManifest:
    def test_raises_when_manifest_file_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            with pytest.raises(
                ManifestNotFoundError,
                match=f"{MANIFEST_FILE_NAME} was not found in directory",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_is_empty(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            (project_dir / MANIFEST_FILE_NAME).unlink()
            (project_dir / MANIFEST_FILE_NAME).touch()
            with pytest.raises(
                InvalidManifestError,
                match="Manifest file is empty or invalid",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_has_no_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "definition": "v1"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_file_has_wrong_type(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "2.0", "type": "spcs"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=f"Manifest file is defined for type spcs. Expected {DCM_PROJECT_TYPE}",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_invalid(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"manifest_version": "1", "type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version '1' is not supported.*>= 2.0 and < 3.0",
            ):
                DCMManifest.load(SecurePath(project_dir))

    def test_raises_when_manifest_version_is_missing(self, project_directory):
        with project_directory("dcm_project") as project_dir:
            with open((project_dir / MANIFEST_FILE_NAME), "w") as f:
                yaml.dump({"type": "dcm_project"}, f)
            with pytest.raises(
                InvalidManifestError,
                match=r"Manifest version '' is not supported.*>= 2.0 and < 3.0",
            ):
                DCMManifest.load(SecurePath(project_dir))


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
            "manifest_version": "2.0",
            "type": "dcm_project",
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()
        (sources_dir / "custom_query.sql").write_text("SELECT 1;")

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
            yaml.dump({"manifest_version": "2.0", "type": "dcm_project"}, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()

        definitions_dir = sources_dir / "definitions"
        definitions_dir.mkdir()
        (definitions_dir / "table.sql").write_text("SELECT 1;")

        macros_dir = sources_dir / "macros"
        macros_dir.mkdir()
        (macros_dir / "helpers.sql").write_text("-- macro")
        (macros_dir / "utils.jinja").write_text("{% macro test() %}{% endmacro %}")

        (sources_dir / "dbt_project.yml").write_text("name: test")

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
                "defaults": {"db_name": "shared_db"},
                "configurations": {
                    "dev": {"suffix": "_dev"},
                    "prod": {"suffix": ""},
                },
            },
        }
        manifest_file = source_dir / MANIFEST_FILE_NAME
        with open(manifest_file, "w") as f:
            yaml.dump(manifest_content, f)

        sources_dir = source_dir / SOURCES_FOLDER
        sources_dir.mkdir()
        (sources_dir / "table.sql").write_text("SELECT 1;")

        DCMProjectManager.sync_local_files(
            project_identifier=TEST_PROJECT, source_directory=str(source_dir)
        )

        mock_sync_artifacts_with_stage.assert_called_once()
        call_args = mock_sync_artifacts_with_stage.call_args
        artifacts = call_args.kwargs["artifacts"]
        artifact_srcs = [a.src for a in artifacts]
        assert MANIFEST_FILE_NAME in artifact_srcs
        assert any(SOURCES_FOLDER in src for src in artifact_srcs)
