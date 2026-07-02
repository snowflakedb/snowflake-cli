import json
import os
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dbt.constants import (
    DBT_PROJECTS_PROFILES_FILENAME,
    ENV_FILENAME,
    PROFILES_FILENAME,
    SUPPORTED_DBT_VERSIONS_QUERY,
)
from snowflake.cli._plugins.dbt.manager import DBTDeployAttributes, DBTManager
from snowflake.cli.api.exceptions import CliArgumentError, CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import ProgrammingError

from tests_common import IS_WINDOWS


def _supported_versions_payload(*versions: str) -> str:
    return json.dumps([{"dbt_version": v} for v in versions])


class TestDeploy:
    def _generate_profile(self, project_path, profile):
        dbt_profiles_file = project_path / PROFILES_FILENAME
        dbt_profiles_file.write_text(yaml.dump(profile))

    @pytest.fixture
    def mock_get_cli_context(self, mock_connect):
        with mock.patch(
            "snowflake.cli.api.cli_global_context.get_cli_context"
        ) as cli_context:
            mock_connect.database = "TestDB"
            mock_connect.schema = "TestSchema"
            cli_context().connection = mock_connect
            yield cli_context()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_dbt_from_default_directories(
        self,
        mock_put_recursive,
        mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}"
        mock_execute_query.assert_called_once_with(expected_query)
        mock_create.assert_called_once_with(mock_from_resource(), temporary=True)
        mock_put_recursive.assert_called_once()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_accepts_dbt_projects_profiles_file(
        self,
        _mock_put_recursive,
        _mock_create,
        project_path,
        profile,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        enable_dbt_projects_profiles_file,
    ):
        """
        What: End-to-end, deploy accepts a project whose profiles come from
              dbt_projects_profiles.yml (no profiles.yml) when the flag is on.
        How: Build a source dir with dbt_project.yml (profile: dev) and only
             dbt_projects_profiles.yml; enable the flag; call deploy with the
             stage operations mocked.
        Expected: deploy completes and issues CREATE DBT PROJECT — i.e. both
                  validation and staging accepted dbt_projects_profiles.yml.
        """
        (project_path / "dbt_project.yml").write_text(yaml.dump({"profile": "dev"}))
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, profile)

        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(project_path),
            profiles_path=SecurePath(project_path),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_force_flag_uses_create_or_replace(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=True,
            attrs=DBTDeployAttributes(),
        )

        expected_query = (
            f"CREATE OR REPLACE DBT PROJECT test_project\nFROM {mock_from_resource()}"
        )
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_alters_existing_object(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        expected_query = (
            f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    @mock.patch("snowflake.cli.api.identifiers.time.time", return_value=1234567890)
    def test_deploys_project_with_case_sensitive_name(
        self,
        mock_time,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string('"MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"'),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        expected_query = f'CREATE DBT PROJECT "MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"\nFROM @"MockDaTaBaSe"."PuBlIc".DBT_PROJECT_caseSenSITIVEnAME_{mock_time()}_STAGE'
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_with_external_access_integrations(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(
                external_access_integrations=[
                    "google_apis_access_integration",
                    "dbt_hub_integration",
                ],
            ),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}\nEXTERNAL_ACCESS_INTEGRATIONS = (google_apis_access_integration, dbt_hub_integration)"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_alter_project_with_external_access_integrations(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": None,
        }
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(
                external_access_integrations=[
                    "google_apis_access_integration",
                    "dbt_hub_integration",
                ],
            ),
        )

        assert mock_execute_query.call_count == 2
        calls = mock_execute_query.call_args_list
        assert (
            calls[0][0][0]
            == "ALTER DBT PROJECT test_project SET EXTERNAL_ACCESS_INTEGRATIONS=(dbt_hub_integration, google_apis_access_integration)"
        )
        assert (
            calls[1][0][0]
            == f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_alter_project_with_both_default_target_and_external_access_integrations(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": "dev",
            "external_access_integrations": ["old_integration"],
        }
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(
                default_target="prod",
                external_access_integrations=[
                    "google_apis_access_integration",
                    "dbt_hub_integration",
                ],
            ),
        )

        assert mock_execute_query.call_count == 2
        calls = mock_execute_query.call_args_list
        assert (
            calls[0][0][0]
            == "ALTER DBT PROJECT test_project SET DEFAULT_TARGET='prod', EXTERNAL_ACCESS_INTEGRATIONS=(dbt_hub_integration, google_apis_access_integration)"
        )
        assert (
            calls[1][0][0]
            == f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_alter_does_not_update_unchanged_external_access_integrations(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": [
                "google_apis_access_integration",
                "dbt_hub_integration",
            ],
        }
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(
                external_access_integrations=[
                    "google_apis_access_integration",
                    "dbt_hub_integration",
                ],
            ),
        )

        assert mock_execute_query.call_count == 1
        calls = mock_execute_query.call_args_list
        assert (
            calls[0][0][0]
            == f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_with_only_local_deps(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(install_local_deps=True),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}\nEXTERNAL_ACCESS_INTEGRATIONS = ()"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_with_local_and_external_deps(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        manager = DBTManager()

        manager.deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(
                external_access_integrations=["github_integration"],
                install_local_deps=True,
            ),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}\nEXTERNAL_ACCESS_INTEGRATIONS = (github_integration)"
        mock_execute_query.assert_called_once_with(expected_query)

    def test_deploy_raises_when_dbt_project_yml_is_not_available(
        self, dbt_project_path
    ):
        dbt_file = dbt_project_path / "dbt_project.yml"
        dbt_file.unlink()

        with pytest.raises(CliError) as exc_info:
            DBTManager().deploy(
                fqn=FQN.from_string("TEST_PIPELINE"),
                path=SecurePath(dbt_project_path),
                profiles_path=SecurePath(dbt_project_path),
                force=False,
                attrs=DBTDeployAttributes(),
            )

        assert "dbt_project.yml does not exist in directory" in exc_info.value.message

    def test_deploy_raises_when_dbt_project_yml_does_not_specify_profile(
        self, dbt_project_path
    ):
        with open((dbt_project_path / "dbt_project.yml"), "w") as f:
            yaml.dump({}, f)

        with pytest.raises(CliError) as exc_info:
            DBTManager().deploy(
                fqn=FQN.from_string("TEST_PIPELINE"),
                path=SecurePath(dbt_project_path),
                profiles_path=SecurePath(dbt_project_path),
                force=False,
                attrs=DBTDeployAttributes(),
            )

        assert "`profile` is not defined in dbt_project.yml" in exc_info.value.message

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_with_default_target(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_target="prod"),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()} DEFAULT_TARGET='prod'"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_existing_project_sets_default_target(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": "dev",
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_target="prod"),
        )

        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert "ALTER DBT PROJECT TEST_PIPELINE SET DEFAULT_TARGET='prod'" in calls[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_existing_project_with_same_default_target(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": "prod",
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_target="prod"),
        )

        query = mock_execute_query.call_args_list[0].args[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in query
        )
        assert len(mock_execute_query.call_args_list) == 1

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_unset_default_target_when_project_exists_with_target(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": "prod",
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(unset_default_target=True),
        )

        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert "ALTER DBT PROJECT TEST_PIPELINE UNSET DEFAULT_TARGET" in calls[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_unset_default_target_when_project_exists_without_target(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(unset_default_target=True),
        )

        query = mock_execute_query.call_args_list[0].args[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in query
        )
        assert len(mock_execute_query.call_args_list) == 1

    def test_validate_profiles_raises_when_file_does_not_exist(
        self, mock_validate_role, project_path
    ):
        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        assert (
            exc_info.value.message
            == f"profiles.yml does not exist in directory {project_path.absolute()}."
        )

    def test_validate_profiles_raises_when_profile_is_not_in_the_file(
        self, mock_validate_role, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "another_profile_name"
            )

        assert (
            exc_info.value.message
            == "Profile another_profile_name is not defined in profiles.yml."
        )

    def test_validate_profiles_raises_when_required_fields_are_missing(
        self, mock_validate_role, project_path, profile
    ):
        profile["dev"]["outputs"]["local"].pop("database", None)
        profile["dev"]["outputs"]["local"].pop("role", None)
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        expected_error_message = """Found following errors in profiles.yml. Please fix them before proceeding:
dev
 * Missing required fields: database, role in target local"""
        assert exc_info.value.message == dedent(expected_error_message)

    def test_prepare_profiles_file_replaces_existing_symlink_with_file(
        self, tmp_path_factory, profile, tmpdir
    ):
        profiles_path = tmp_path_factory.mktemp("profiles")
        dbt_profiles_file = profiles_path / "profiles_real.yml"
        dbt_profiles_file.write_text(yaml.dump(profile))
        os.symlink(dbt_profiles_file.absolute(), profiles_path / PROFILES_FILENAME)
        assert (profiles_path / PROFILES_FILENAME).is_symlink() is True

        tmp_dbt_path = Path(tmpdir)
        tmp_profiles_file = tmp_dbt_path / PROFILES_FILENAME
        os.symlink(dbt_profiles_file, tmp_profiles_file)
        assert tmp_profiles_file.is_symlink() is True

        DBTManager._prepare_profiles_file(profiles_path, tmp_dbt_path)  # noqa: SLF001

        assert tmp_profiles_file.is_symlink() is False

    def test_prepare_profiles_file_removes_all_comments(
        self, tmp_path_factory, profile
    ):
        profiles_path = tmp_path_factory.mktemp("profiles")
        dbt_profiles_file = profiles_path / PROFILES_FILENAME
        # not a comment - valid ones start exactly with `# `
        profile["#key"] = "#value"
        dbt_profiles_file.write_text(yaml.dump(profile))
        with open(dbt_profiles_file, "a") as fp:
            fp.write("# full line comment\n")
            fp.write("key: with # comment\n")
            fp.write("another_key: with # comment # and one more # and more\n")
            fp.write("#         password: 123\n")

        tmp_dbt_path = tmp_path_factory.mktemp("dbt")

        DBTManager._prepare_profiles_file(profiles_path, tmp_dbt_path)  # noqa: SLF001

        assert tmp_dbt_path.is_symlink() is False
        with open(tmp_dbt_path / PROFILES_FILENAME, "r") as fp:
            actual = yaml.safe_load(fp)
        expected = profile | {"key": "with", "another_key": "with"}
        assert actual == expected

        # pyyaml ignores comments, so as safety net we need to check that comments were removed on lower level
        with open(tmp_dbt_path / PROFILES_FILENAME, "r") as fp:
            for line in fp:
                assert "comment" not in line
                assert "password" not in line
                assert "# " not in line

    def test_validate_profiles_with_valid_default_target(
        self, mock_validate_role, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        DBTManager()._validate_profiles(  # noqa: SLF001
            SecurePath(project_path), "dev", "prod"
        )

    def test_validate_profiles_with_invalid_default_target(
        self, mock_validate_role, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev", "invalid_target"
            )

        assert (
            "Target 'invalid_target' is not defined in profile 'dev'"
            in exc_info.value.message
        )
        assert "Available targets: local, prod" in exc_info.value.message

    def test_validate_profiles_without_default_target(
        self, mock_validate_role, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        DBTManager()._validate_profiles(  # noqa: SLF001
            SecurePath(project_path), "dev", None
        )

    def test_validate_profiles_with_existing_role(
        self, mock_validate_role, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        DBTManager()._validate_profiles(  # noqa: SLF001
            SecurePath(project_path), "dev", None
        )

        mock_validate_role.assert_called_once()
        assert mock_validate_role.call_args[1]["role_name"] == "test_role"

    def test_validate_profiles_with_nonexistent_role(
        self, mock_validate_role, project_path, profile
    ):
        mock_validate_role.return_value = False

        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev", None
            )

        assert "does not exist or is not accessible" in exc_info.value.message
        assert "test_role" in exc_info.value.message

    @pytest.mark.parametrize(
        "role_expr",
        [
            "{{ env_var('DBT_ROLE') }}",
            '{{ env_var("DBT_ROLE") }}',
            "{{ env_var('DBT_ROLE', 'default_role') }}",
            "{{env_var('DBT_ROLE')}}",
            "{{ select CURRENT_ROLE() }}",
            "{{ some_unknown_expr }}",
        ],
    )
    def test_validate_profiles_skips_role_validation_for_jinja_expression(
        self, mock_validate_role, project_path, profile, role_expr
    ):
        profile["dev"]["outputs"]["local"]["role"] = role_expr
        self._generate_profile(project_path, profile)

        DBTManager()._validate_profiles(  # noqa: SLF001
            SecurePath(project_path), "dev", None
        )

        mock_validate_role.assert_not_called()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_with_dbt_version(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        mock_validate_dbt_version,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(dbt_version="1.9.0"),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()} DBT_VERSION='1.9.0'"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_or_replace_with_dbt_version(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        mock_validate_dbt_version,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=True,
            attrs=DBTDeployAttributes(dbt_version="2.0.0"),
        )

        expected_query = f"CREATE OR REPLACE DBT PROJECT test_project\nFROM {mock_from_resource()} DBT_VERSION='2.0.0'"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_alter_with_dbt_version_change(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        mock_validate_dbt_version,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": None,
            "dbt_version": "1.8.0",
        }

        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(dbt_version="1.9.0"),
        )

        assert mock_execute_query.call_count == 2
        calls = mock_execute_query.call_args_list
        assert (
            calls[0][0][0] == "ALTER DBT PROJECT test_project SET DBT_VERSION='1.9.0'"
        )
        assert (
            calls[1][0][0]
            == f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_alter_with_same_dbt_version_does_update(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        mock_validate_dbt_version,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "external_access_integrations": None,
            "dbt_version": "1.9.0",
        }

        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(dbt_version="1.9.0"),
        )

        assert mock_execute_query.call_count == 2
        calls = mock_execute_query.call_args_list
        assert (
            calls[0][0][0] == "ALTER DBT PROJECT test_project SET DBT_VERSION='1.9.0'"
        )
        assert (
            calls[1][0][0]
            == f"ALTER DBT PROJECT test_project ADD VERSION\nFROM {mock_from_resource()}"
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_aborts_before_upload_when_version_unsupported(
        self,
        mock_put_recursive,
        mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_cursor,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(_supported_versions_payload("1.9.4", "1.10.15"),)],
            columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"],
        )

        with pytest.raises(CliArgumentError) as exc_info:
            DBTManager().deploy(
                fqn=FQN.from_string("test_project"),
                path=SecurePath(dbt_project_path),
                profiles_path=SecurePath(dbt_project_path),
                force=False,
                attrs=DBTDeployAttributes(dbt_version="99.99.99"),
            )

        msg = exc_info.value.message
        assert "99.99.99" in msg
        assert "1.9.4" in msg
        assert "1.10.15" in msg
        mock_execute_query.assert_called_once_with(SUPPORTED_DBT_VERSIONS_QUERY)
        mock_create.assert_not_called()
        mock_put_recursive.assert_not_called()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_does_not_validate_when_version_not_specified(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
        mock_validate_dbt_version,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        mock_validate_dbt_version.assert_not_called()

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_with_default_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_env="dev"),
        )

        expected_query = f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()} DEFAULT_ENVIRONMENT='dev'"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_with_default_target_and_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_target="prod", default_env="dev"),
        )

        expected_query = (
            f"CREATE DBT PROJECT test_project\nFROM {mock_from_resource()}"
            f" DEFAULT_TARGET='prod' DEFAULT_ENVIRONMENT='dev'"
        )
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_create_or_replace_with_default_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=True,
            attrs=DBTDeployAttributes(default_env="prod"),
        )

        expected_query = f"CREATE OR REPLACE DBT PROJECT test_project\nFROM {mock_from_resource()} DEFAULT_ENVIRONMENT='prod'"
        mock_execute_query.assert_called_once_with(expected_query)

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_existing_project_sets_default_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "default_env": None,
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_env="dev"),
        )

        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert (
            "ALTER DBT PROJECT TEST_PIPELINE SET DEFAULT_ENVIRONMENT='dev'" in calls[0]
        )
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_existing_project_with_same_default_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "default_env": "dev",
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(default_env="dev"),
        )

        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert (
            "ALTER DBT PROJECT TEST_PIPELINE SET DEFAULT_ENVIRONMENT='dev'" in calls[0]
        )
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_unset_default_environment_when_project_exists_with_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "default_env": "dev",
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(unset_default_env=True),
        )

        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert "ALTER DBT PROJECT TEST_PIPELINE UNSET DEFAULT_ENVIRONMENT" in calls[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_unset_default_environment_when_project_exists_without_environment(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        mock_get_dbt_object_attributes.return_value = {
            "default_target": None,
            "default_env": None,
            "external_access_integrations": None,
        }

        DBTManager().deploy(
            fqn=FQN.from_string("TEST_PIPELINE"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            force=False,
            attrs=DBTDeployAttributes(unset_default_env=True),
        )

        # We always emit UNSET when requested; the server treats it as a no-op
        # if the property is already null.
        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert "ALTER DBT PROJECT TEST_PIPELINE UNSET DEFAULT_ENVIRONMENT" in calls[0]
        assert (
            f"ALTER DBT PROJECT TEST_PIPELINE ADD VERSION\nFROM {mock_from_resource()}"
            in calls[1]
        )

    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_with_env_file_dir_overwrites_existing_env_yml(
        self,
        _mock_put_recursive,
        _mock_create,
        dbt_project_path,
        env_yml_dir,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        # An env.yml inside the source dir should be overwritten by the one
        # injected from --env-file-dir.
        (dbt_project_path / ENV_FILENAME).write_text(
            yaml.dump({"env_config": {"environments": [{"name": "from_source"}]}})
        )

        DBTManager().deploy(
            fqn=FQN.from_string("test_project"),
            path=SecurePath(dbt_project_path),
            profiles_path=SecurePath(dbt_project_path),
            env_file_path=SecurePath(env_yml_dir),
            force=False,
            attrs=DBTDeployAttributes(),
        )

        mock_execute_query.assert_called_once()

    def test_deploy_raises_when_env_file_dir_missing_env_yml(
        self, dbt_project_path, tmp_path_factory, mock_validate_role
    ):
        empty_dir = tmp_path_factory.mktemp("empty_envs")

        with pytest.raises(CliError) as exc_info:
            DBTManager().deploy(
                fqn=FQN.from_string("TEST_PIPELINE"),
                path=SecurePath(dbt_project_path),
                profiles_path=SecurePath(dbt_project_path),
                env_file_path=SecurePath(empty_dir),
                force=False,
                attrs=DBTDeployAttributes(),
            )

        assert f"{ENV_FILENAME} does not exist in directory" in exc_info.value.message

    @pytest.mark.parametrize(
        "bad_env_yml, expected_message",
        [
            pytest.param(
                "env_config:\n  environments:\n  - name: dev\n    env:\n"
                "      DBT_FOO: 'a'\n      DBT_FOO: 'b'\n",
                "duplicate key",
                id="duplicate-key",
            ),
            pytest.param(
                "env_config:\n  environments\n    - bad: : indentation\n",
                "is not valid YAML",
                id="malformed-yaml",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "from_env_file_dir",
        [pytest.param(False, id="source-dir"), pytest.param(True, id="env-file-dir")],
    )
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.create")
    @mock.patch("snowflake.cli._plugins.dbt.manager.StageManager.put_recursive")
    def test_deploy_validates_env_file_before_stage_creation(
        self,
        mock_put_recursive,
        mock_create,
        from_env_file_dir,
        bad_env_yml,
        expected_message,
        dbt_project_path,
        tmp_path_factory,
        mock_get_dbt_object_attributes,
        mock_execute_query,
        mock_get_cli_context,
        mock_from_resource,
        mock_validate_role,
    ):
        # A bad env.yml from either source (explicit --env-file-dir or the
        # source directory) must be validated before any temporary stage is
        # created.
        if from_env_file_dir:
            env_dir = tmp_path_factory.mktemp("envs")
            (env_dir / ENV_FILENAME).write_text(bad_env_yml)
            env_file_path = SecurePath(env_dir)
        else:
            (dbt_project_path / ENV_FILENAME).write_text(bad_env_yml)
            env_file_path = None

        with pytest.raises(CliError) as exc_info:
            DBTManager().deploy(
                fqn=FQN.from_string("test_project"),
                path=SecurePath(dbt_project_path),
                profiles_path=SecurePath(dbt_project_path),
                env_file_path=env_file_path,
                force=False,
                attrs=DBTDeployAttributes(),
            )

        assert expected_message in exc_info.value.message
        mock_create.assert_not_called()
        mock_put_recursive.assert_not_called()

    def test_write_env_file_replaces_existing_file(self, tmp_path_factory, env_yml):
        env_path = tmp_path_factory.mktemp("envs")
        (env_path / ENV_FILENAME).write_text(yaml.dump(env_yml))

        tmp_dbt_path = tmp_path_factory.mktemp("dbt")
        (tmp_dbt_path / ENV_FILENAME).write_text("stale content from --source\n")

        content = DBTManager._validate_and_parse_env_file(  # noqa: SLF001
            SecurePath(env_path / ENV_FILENAME)
        )
        DBTManager._write_env_file(content, tmp_dbt_path)  # noqa: SLF001

        with open(tmp_dbt_path / ENV_FILENAME) as fp:
            actual = yaml.safe_load(fp)
        assert actual == env_yml

    def test_load_and_write_env_file_removes_comments(self, tmp_path_factory, env_yml):
        env_path = tmp_path_factory.mktemp("envs")
        env_file = env_path / ENV_FILENAME
        env_file.write_text(yaml.dump(env_yml))
        with open(env_file, "a") as fp:
            fp.write("# secret hint left by developer\n")
            fp.write("extra_key: with # trailing comment\n")

        tmp_dbt_path = tmp_path_factory.mktemp("dbt")

        content = DBTManager._validate_and_parse_env_file(  # noqa: SLF001
            SecurePath(env_file)
        )
        DBTManager._write_env_file(content, tmp_dbt_path)  # noqa: SLF001

        with open(tmp_dbt_path / ENV_FILENAME) as fp:
            for line in fp:
                assert "# " not in line
                assert "secret hint" not in line

    def test_validate_and_parse_env_file_rejects_duplicate_keys(self, tmp_path_factory):
        env_path = tmp_path_factory.mktemp("envs")
        (env_path / ENV_FILENAME).write_text(
            "env_config:\n"
            "  environments:\n"
            "  - name: dev\n"
            "    env:\n"
            "      DBT_FOO: 'first'\n"
            "      DBT_FOO: 'second'\n"
        )

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_and_parse_env_file(  # noqa: SLF001
                SecurePath(env_path / ENV_FILENAME)
            )

        assert "duplicate key" in exc_info.value.message
        assert "DBT_FOO" in exc_info.value.message

    def test_validate_and_parse_env_file_rejects_malformed_yaml(self, tmp_path_factory):
        env_path = tmp_path_factory.mktemp("envs")
        (env_path / ENV_FILENAME).write_text(
            "env_config:\n" "  environments\n" "    - bad: : indentation\n"
        )

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_and_parse_env_file(  # noqa: SLF001
                SecurePath(env_path / ENV_FILENAME)
            )

        assert "is not valid YAML" in exc_info.value.message


class TestValidateDbtVersion:
    def test_get_supported_dbt_versions_parses_response(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(_supported_versions_payload("1.9.4", "1.10.15"),)],
            columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"],
        )

        result = DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

        assert result == ["1.9.4", "1.10.15"]
        mock_execute_query.assert_called_once_with(SUPPORTED_DBT_VERSIONS_QUERY)

    def test_get_supported_dbt_versions_raises_on_empty_result(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[], columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"]
        )

        with pytest.raises(CliError, match="Could not fetch supported dbt versions"):
            DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

    def test_get_supported_dbt_versions_raises_on_null_value(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(None,)], columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"]
        )

        with pytest.raises(CliError, match="Could not fetch supported dbt versions"):
            DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

    def test_get_supported_dbt_versions_raises_on_invalid_json(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[("not json",)], columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"]
        )

        with pytest.raises(CliError, match="Could not parse supported dbt versions"):
            DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

    def test_get_supported_dbt_versions_raises_on_programming_error(
        self, mock_execute_query
    ):
        mock_execute_query.side_effect = ProgrammingError("Unknown function")

        with pytest.raises(
            CliError,
            match="Ensure your Snowflake account supports SYSTEM\\$SUPPORTED_DBT_VERSIONS",
        ):
            DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

    def test_get_supported_dbt_versions_raises_on_empty_list(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(_supported_versions_payload(),)],
            columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"],
        )

        with pytest.raises(CliError, match="Server returned no supported dbt versions"):
            DBTManager()._get_supported_dbt_versions()  # noqa: SLF001

    def test_validate_dbt_version_passes_for_supported_version(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(_supported_versions_payload("1.9.4"),)],
            columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"],
        )

        DBTManager()._validate_dbt_version("1.9.4")  # noqa: SLF001

    def test_validate_dbt_version_raises_for_unsupported_version(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[(_supported_versions_payload("1.9.4", "1.10.15"),)],
            columns=["SYSTEM$SUPPORTED_DBT_VERSIONS()"],
        )

        with pytest.raises(CliArgumentError) as exc_info:
            DBTManager()._validate_dbt_version("99.99.99")  # noqa: SLF001

        msg = exc_info.value.message
        assert "99.99.99" in msg
        assert "1.9.4" in msg
        assert "1.10.15" in msg


class TestGetDBTObjectAttributes:
    @pytest.fixture
    def mock_describe(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.describe",
            return_value=mock.MagicMock(),
        ) as _fixture:
            yield _fixture

    def test_get_dbt_object_attributes_when_object_does_not_exist(self, mock_describe):
        fqn = FQN.from_string("test_project")

        mock_describe.side_effect = ProgrammingError(
            f"002003 (02000): 01bec8ce-010b-16e8-0000-5349394c206e: SQL compilation error:\nDBT PROJECT '{fqn.name}' does not exist or not authorized."
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is None

    def test_get_dbt_object_attributes_when_no_rows_returned(self, mock_describe):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.__iter__ = mock.MagicMock(return_value=iter([]))

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is None

    def test_get_dbt_object_attributes_with_default_target(self, mock_describe):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.description = [("default_target",), ("other_field",)]
        mock_row = ("prod", "other_value")
        mock_describe.return_value.__iter__ = mock.MagicMock(
            return_value=iter([mock_row])
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is not None
        assert result["default_target"] == "prod"

    def test_get_dbt_object_attributes_with_null_default_target(self, mock_describe):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.description = [("default_target",), ("other_field",)]
        mock_row = (None, "other_value")
        mock_describe.return_value.__iter__ = mock.MagicMock(
            return_value=iter([mock_row])
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is not None
        assert result["default_target"] is None

    def test_get_dbt_object_attributes_missing_default_target_column(
        self, mock_describe
    ):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.description = [("other_field",), ("another_field",)]
        mock_row = ("value1", "value2")
        mock_describe.return_value.__iter__ = mock.MagicMock(
            return_value=iter([mock_row])
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is not None
        assert (
            result["default_target"] is None
        )  # Should default to None when key is missing

    def test_get_dbt_object_attributes_with_dbt_version(self, mock_describe):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.description = [
            ("dbt_version",),
        ]
        mock_row = ("1.9.0",)
        mock_describe.return_value.__iter__ = mock.MagicMock(
            return_value=iter([mock_row])
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is not None
        assert result["dbt_version"] == "1.9.0"

    def test_get_dbt_object_attributes_with_default_environment(self, mock_describe):
        fqn = FQN.from_string("test_project")
        mock_describe.return_value.description = [("default_environment",)]
        mock_row = ("dev",)
        mock_describe.return_value.__iter__ = mock.MagicMock(
            return_value=iter([mock_row])
        )

        result = DBTManager.get_dbt_object_attributes(fqn)

        assert result is not None
        assert result["default_env"] == "dev"


class TestValidateRole:
    @pytest.fixture
    def mock_current_role(self):
        with mock.patch(
            "snowflake.cli._plugins.dbt.manager.DBTManager.current_role"
        ) as _fixture:
            _fixture.return_value = "original_role"
            yield _fixture

    def test_validate_role_returns_true_when_role_is_valid(
        self, mock_execute_query, mock_current_role
    ):

        result = DBTManager()._validate_role("test_role")  # noqa: SLF001

        assert result is True
        assert mock_execute_query.call_count == 3
        calls = [call.args[0] for call in mock_execute_query.call_args_list]
        assert calls[0] == "use role test_role"
        assert calls[1] == "select 1"
        assert calls[2] == "use role original_role"

    def test_validate_role_returns_false_when_programming_error_raised(
        self, mock_execute_query, mock_current_role
    ):
        mock_execute_query.side_effect = ProgrammingError("Role does not exist")

        result = DBTManager()._validate_role("invalid_role")  # noqa: SLF001

        assert result is False
        mock_execute_query.assert_called_once_with("use role invalid_role")


class TestExecute:
    @pytest.mark.parametrize(
        "kwargs,extra_args,expected_query",
        [
            pytest.param(
                {"environment": "dev"},
                (),
                "EXECUTE DBT PROJECT pipeline ENVIRONMENT='dev' args='run'",
                id="environment-only",
            ),
            pytest.param(
                {"environment": "NO_ENV"},
                (),
                "EXECUTE DBT PROJECT pipeline ENVIRONMENT='NO_ENV' args='run'",
                id="environment-no-env-sentinel",
            ),
            pytest.param(
                {"env_vars": '{"DBT_FOO": "1"}'},
                (),
                "EXECUTE DBT PROJECT pipeline ENV_VARS=('DBT_FOO'='1') args='run'",
                id="env-vars-json-single",
            ),
            pytest.param(
                {"env_vars": '{"DBT_FOO": "1", "DBT_BAR": "2"}'},
                (),
                "EXECUTE DBT PROJECT pipeline "
                "ENV_VARS=('DBT_FOO'='1', 'DBT_BAR'='2') args='run'",
                id="env-vars-json-multi",
            ),
            pytest.param(
                {"env_vars": "{DBT_FOO: '1', DBT_BAR: '2'}"},
                (),
                "EXECUTE DBT PROJECT pipeline "
                "ENV_VARS=('DBT_FOO'='1', 'DBT_BAR'='2') args='run'",
                id="env-vars-yaml-quoted-strings",
            ),
            pytest.param(
                {"env_vars": '{"DBT_URL": "https://example.com/?a=b"}'},
                (),
                "EXECUTE DBT PROJECT pipeline "
                "ENV_VARS=('DBT_URL'='https://example.com/?a=b') args='run'",
                id="env-vars-value-with-equals",
            ),
            pytest.param(
                {"env_vars": 'DBT_MSG: "it\'s"'},
                (),
                "EXECUTE DBT PROJECT pipeline "
                "ENV_VARS=('DBT_MSG'='it''s') args='run'",
                id="env-vars-value-with-single-quote-escaped",
            ),
            pytest.param(
                {
                    "dbt_version": "1.9.0",
                    "environment": "prod",
                    "env_vars": '{"DBT_FOO": "1"}',
                },
                (),
                "EXECUTE DBT PROJECT pipeline dbt_version='1.9.0' "
                "ENVIRONMENT='prod' ENV_VARS=('DBT_FOO'='1') args='run'",
                id="all-options-ordering",
            ),
        ],
    )
    def test_execute_builds_expected_sql(
        self, mock_execute_query, kwargs, extra_args, expected_query
    ):
        DBTManager().execute(
            "run",
            FQN.from_string("pipeline"),
            False,
            kwargs.get("dbt_version"),
            kwargs.get("environment"),
            kwargs.get("env_vars"),
            *extra_args,
        )

        mock_execute_query.assert_called_once_with(expected_query, _exec_async=False)

    def test_execute_no_env_options_omits_clauses(self, mock_execute_query):
        DBTManager().execute("run", FQN.from_string("pipeline"), False)

        mock_execute_query.assert_called_once_with(
            "EXECUTE DBT PROJECT pipeline args='run'", _exec_async=False
        )

    @pytest.mark.parametrize(
        "raw_value,expected_error",
        [
            pytest.param(
                '"just_a_string"',
                "must be a YAML/JSON object",
                id="non-mapping-string",
            ),
            pytest.param(
                "[1, 2, 3]",
                "must be a YAML/JSON object",
                id="non-mapping-list",
            ),
            pytest.param(
                '{"DBT_X": null}',
                "must not be null",
                id="null-value",
            ),
            pytest.param(
                '{"DBT_X": 1}',
                "must be a string",
                id="int-value",
            ),
            pytest.param(
                '{"DBT_X": 1.5}',
                "must be a string",
                id="float-value",
            ),
            pytest.param(
                '{"DBT_X": true}',
                "must be a string",
                id="bool-value",
            ),
            pytest.param(
                '{"DBT_X": {"nested": "1"}}',
                "must be a string",
                id="nested-object",
            ),
            pytest.param(
                '{"DBT_X": ["1", "2"]}',
                "must be a string",
                id="nested-array",
            ),
            pytest.param(
                "{not: valid: yaml: at: all",
                "must be valid YAML/JSON",
                id="malformed-yaml",
            ),
            pytest.param(
                '{"DBT_FOO": "1", "DBT_FOO": "2"}',
                "duplicate key",
                id="duplicate-key",
            ),
            pytest.param(
                '{"": "v"}',
                "must not be empty",
                id="empty-key",
            ),
            pytest.param(
                '{"FOO": "1"}',
                "must start with",
                id="key-missing-dbt-prefix",
            ),
            pytest.param(
                '{"DBT-FOO": "1"}',
                "ASCII letters",
                id="key-invalid-chars-hyphen",
            ),
            pytest.param(
                '{"DBT FOO": "1"}',
                "ASCII letters",
                id="key-invalid-chars-space",
            ),
            pytest.param(
                '{"DBT_FOO": "value\\nwith\\nnewlines"}',
                "must not contain control characters",
                id="value-control-char",
            ),
        ],
    )
    def test_execute_env_vars_invalid_input_raises(
        self, mock_execute_query, raw_value, expected_error
    ):
        with pytest.raises(CliError) as exc:
            DBTManager().execute(
                "run",
                FQN.from_string("pipeline"),
                False,
                None,
                None,
                raw_value,
            )

        assert expected_error in str(exc.value.message)
        mock_execute_query.assert_not_called()

    def test_execute_env_vars_secret_prefix_warns(self, mock_execute_query, capsys):
        DBTManager().execute(
            "run",
            FQN.from_string("pipeline"),
            False,
            None,
            None,
            '{"DBT_ENV_SECRET_TOKEN": "xyz"}',
        )

        captured = capsys.readouterr()
        assert "DBT_ENV_SECRET_" in captured.out
        assert "DBT_ENV_SECRET_TOKEN" in captured.out
        mock_execute_query.assert_called_once_with(
            "EXECUTE DBT PROJECT pipeline "
            "ENV_VARS=('DBT_ENV_SECRET_TOKEN'='xyz') args='run'",
            _exec_async=False,
        )

    def test_execute_async_with_env_vars(self, mock_execute_query):
        DBTManager().execute(
            "compile",
            FQN.from_string("pipeline"),
            True,
            None,
            "dev",
            '{"DBT_FOO": "1"}',
        )

        mock_execute_query.assert_called_once_with(
            "EXECUTE DBT PROJECT pipeline ENVIRONMENT='dev' "
            "ENV_VARS=('DBT_FOO'='1') args='compile'",
            _exec_async=True,
        )

    def test_execute_forwards_shell_env_vars_with_cli_args(
        self, mock_execute_query, clean_dbt_env
    ):
        # use_shell_env_vars is keyword-only, so positional dbt CLI args
        # ("--select", "my_model") still map to *dbt_cli_args rather than
        # being misbound to the flag.
        clean_dbt_env.setenv("DBT_FOO", "1")

        DBTManager().execute(
            "run",
            FQN.from_string("pipeline"),
            False,
            None,
            None,
            None,
            "--select",
            "my_model",
            use_shell_env_vars=True,
        )

        mock_execute_query.assert_called_once_with(
            "EXECUTE DBT PROJECT pipeline "
            "ENV_VARS=('DBT_FOO'='1') args='run --select my_model'",
            _exec_async=False,
        )


class TestCollectShellEnvVars:
    """Direct manager-level coverage of _collect_shell_env_vars."""

    def test_returns_sorted_dict_and_zero_dropped(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_ZULU", "z")
        clean_dbt_env.setenv("DBT_ALPHA", "a")
        clean_dbt_env.setenv("DBT_MIKE", "m")

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert list(forwarded.items()) == [
            ("DBT_ALPHA", "a"),
            ("DBT_MIKE", "m"),
            ("DBT_ZULU", "z"),
        ]
        assert dropped == 0
        assert skipped == 0

    def test_excludes_non_dbt_keys(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_FOO", "1")
        clean_dbt_env.setenv("PATH", "/bin")
        clean_dbt_env.setenv("AWS_ACCESS_KEY", "key")
        clean_dbt_env.setenv("DBTFOO", "no-underscore")  # missing underscore
        clean_dbt_env.setenv("XDBT_FOO", "wrong-prefix")

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {"DBT_FOO": "1"}
        assert dropped == 0
        # DBTFOO and XDBT_FOO do not start with the DBT_ prefix → not DBT-ish,
        # ignored silently (not counted as skipped).
        assert skipped == 0

    def test_drops_secret_prefixed_keys(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_FOO", "1")
        clean_dbt_env.setenv("DBT_ENV_SECRET_TOKEN", "shhh")
        clean_dbt_env.setenv("DBT_ENV_SECRET_API_KEY", "shhh2")

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {"DBT_FOO": "1"}
        assert dropped == 2
        assert skipped == 0

    def test_drops_mixed_case_secret_prefixed_keys(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        # Mixed-case secret prefix is detected case-insensitively and counted
        # as a dropped secret (not a generic skip), so the user gets the
        # secrets-block guidance and the value never reaches query history.
        clean_dbt_env.setenv("DBT_Env_Secret_TOKEN", "shhh")

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {}
        assert dropped == 1
        assert skipped == 0

    @pytest.mark.skipif(
        IS_WINDOWS,
        reason="os.environ is case-insensitive on Windows and normalizes names "
        "to uppercase, so a non-uppercase DBT_ env var cannot exist there and "
        "the skip path is unreachable.",
    )
    def test_skips_non_uppercase_keys(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_FOO", "ok")
        clean_dbt_env.setenv("DBT_Foo", "mixed")  # not uppercase → skipped
        clean_dbt_env.setenv("dbt_bar", "lower")  # DBT-ish but lowercase → skipped

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {"DBT_FOO": "ok"}
        assert dropped == 0
        assert skipped == 2

    def test_skips_invalid_char_keys(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_FOO", "ok")
        clean_dbt_env.setenv("DBT_FOO-BAR", "dash")  # invalid char → skipped

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {"DBT_FOO": "ok"}
        assert dropped == 0
        assert skipped == 1

    def test_skips_control_char_values(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_FOO", "ok")
        clean_dbt_env.setenv("DBT_BAR", "bad\nvalue")  # control char → skipped

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {"DBT_FOO": "ok"}
        assert dropped == 0
        assert skipped == 1

    def test_empty_environment_returns_empty(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {}
        assert dropped == 0
        assert skipped == 0

    def test_only_secrets_returns_empty_dict_with_count(self, clean_dbt_env):
        from snowflake.cli._plugins.dbt.manager import _collect_shell_env_vars

        clean_dbt_env.setenv("DBT_ENV_SECRET_TOKEN", "shhh")

        forwarded, dropped, skipped = _collect_shell_env_vars()

        assert forwarded == {}
        assert dropped == 1
        assert skipped == 0


def _write_profile(directory: Path, filename: str, profile: dict) -> Path:
    """Write a dbt profiles mapping to <directory>/<filename> and return the path."""
    path = directory / filename
    path.write_text(yaml.dump(profile))
    return path


class TestDbtProjectsProfilesFile:
    """Coverage for the ENABLE_FIX_3659937_DBT_PROJECTS_PROFILES_FILE behavior:
    dbt_projects_profiles.yml takes precedence over profiles.yml and is staged
    under its own name, gated behind the feature flag."""

    # --- full flag x files staging matrix (before/after truth table) ---

    @pytest.mark.parametrize(
        "flag_on, dbt_projects_present, profiles_present, expected_staged, expected_error",
        [
            pytest.param(
                False,
                True,
                True,
                PROFILES_FILENAME,
                None,
                id="flag_off-both_present-stages_profiles",
            ),
            pytest.param(
                False,
                True,
                False,
                None,
                PROFILES_FILENAME,
                id="flag_off-only_dbt_projects-errors_profiles_missing",
            ),
            pytest.param(
                False,
                False,
                True,
                PROFILES_FILENAME,
                None,
                id="flag_off-only_profiles-stages_profiles",
            ),
            pytest.param(
                False,
                False,
                False,
                None,
                PROFILES_FILENAME,
                id="flag_off-neither-errors_profiles_missing",
            ),
            pytest.param(
                True,
                True,
                True,
                DBT_PROJECTS_PROFILES_FILENAME,
                None,
                id="flag_on-both_present-stages_dbt_projects",
            ),
            pytest.param(
                True,
                True,
                False,
                DBT_PROJECTS_PROFILES_FILENAME,
                None,
                id="flag_on-only_dbt_projects-stages_dbt_projects",
            ),
            pytest.param(
                True,
                False,
                True,
                PROFILES_FILENAME,
                None,
                id="flag_on-only_profiles-stages_profiles",
            ),
            pytest.param(
                True,
                False,
                False,
                None,
                f"{DBT_PROJECTS_PROFILES_FILENAME} or {PROFILES_FILENAME}",
                id="flag_on-neither-errors_both_names",
            ),
        ],
    )
    def test_staging_matrix(
        self,
        tmp_path_factory,
        profile,
        flag_on,
        dbt_projects_present,
        profiles_present,
        expected_staged,
        expected_error,
    ):
        """
        What: Pin the staging outcome of _prepare_profiles_file across every
              combination of the feature flag and which profiles files are
              present. This is the before/after truth table for the feature.
        How: For each combination, create a profiles dir with the requested
             files, force the flag to the given state, and run
             _prepare_profiles_file into a fresh staging dir.
        Expected: when a file should be staged, the staging dir holds exactly that
                  filename and not the other candidate; when nothing is recognized,
                  a CliError naming the expected file(s) is raised.
        """
        profiles_dir = tmp_path_factory.mktemp("profiles")
        tmp_dir = tmp_path_factory.mktemp("stage")
        if dbt_projects_present:
            _write_profile(profiles_dir, DBT_PROJECTS_PROFILES_FILENAME, profile)
        if profiles_present:
            _write_profile(profiles_dir, PROFILES_FILENAME, profile)

        with mock.patch(
            "snowflake.cli.api.feature_flags.FeatureFlag."
            "ENABLE_FIX_3659937_DBT_PROJECTS_PROFILES_FILE.is_enabled",
            return_value=flag_on,
        ):
            if expected_error is not None:
                with pytest.raises(CliError) as exc_info:
                    DBTManager._prepare_profiles_file(  # noqa: SLF001
                        profiles_dir, tmp_dir
                    )
                assert expected_error in exc_info.value.message
                assert "does not exist" in exc_info.value.message
            else:
                DBTManager._prepare_profiles_file(profiles_dir, tmp_dir)  # noqa: SLF001
                assert (tmp_dir / expected_staged).exists()
                other = (
                    PROFILES_FILENAME
                    if expected_staged == DBT_PROJECTS_PROFILES_FILENAME
                    else DBT_PROJECTS_PROFILES_FILENAME
                )
                assert not (tmp_dir / other).exists()

    # --- resolver / candidate precedence ---

    def test_resolver_ignores_dbt_projects_file_when_flag_disabled(self, project_path):
        """
        What: With the flag off, dbt_projects_profiles.yml is not a recognized
              profiles source — only profiles.yml is.
        How: Write just dbt_projects_profiles.yml into the dir; leave the flag at
             its default (off); call the candidate list and the resolver.
        Expected: candidates == ('profiles.yml',) and the resolver returns None,
                  so today's behavior is preserved byte-for-byte.
        """
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, {"dev": {}})

        assert DBTManager._candidate_profiles_filenames() == (  # noqa: SLF001
            PROFILES_FILENAME,
        )
        assert (
            DBTManager._resolve_profiles_filename(project_path) is None  # noqa: SLF001
        )

    def test_resolver_returns_profiles_yml_when_only_that_present(
        self, project_path, enable_dbt_projects_profiles_file
    ):
        """
        What: With the flag on and only profiles.yml present, profiles.yml is
              resolved.
        How: Write only profiles.yml; enable the flag; call the resolver.
        Expected: resolver returns 'profiles.yml'.
        """
        _write_profile(project_path, PROFILES_FILENAME, {"dev": {}})

        assert (
            DBTManager._resolve_profiles_filename(project_path)  # noqa: SLF001
            == PROFILES_FILENAME
        )

    def test_resolver_returns_dbt_projects_file_when_only_that_present(
        self, project_path, enable_dbt_projects_profiles_file
    ):
        """
        What: With the flag on and only dbt_projects_profiles.yml present, it is
              resolved.
        How: Write only dbt_projects_profiles.yml; enable the flag; call resolver.
        Expected: resolver returns 'dbt_projects_profiles.yml'.
        """
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, {"dev": {}})

        assert (
            DBTManager._resolve_profiles_filename(project_path)  # noqa: SLF001
            == DBT_PROJECTS_PROFILES_FILENAME
        )

    def test_resolver_prefers_dbt_projects_file_when_both_present(
        self, project_path, enable_dbt_projects_profiles_file
    ):
        """
        What: dbt_projects_profiles.yml wins over profiles.yml when both exist.
        How: Write both files; enable the flag; inspect candidate order and the
             resolver result.
        Expected: candidates lead with dbt_projects_profiles.yml and the resolver
                  returns it.
        """
        _write_profile(project_path, PROFILES_FILENAME, {"dev": {}})
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, {"dev": {}})

        assert DBTManager._candidate_profiles_filenames() == (  # noqa: SLF001
            DBT_PROJECTS_PROFILES_FILENAME,
            PROFILES_FILENAME,
        )
        assert (
            DBTManager._resolve_profiles_filename(project_path)  # noqa: SLF001
            == DBT_PROJECTS_PROFILES_FILENAME
        )

    def test_resolver_returns_none_when_neither_present(
        self, project_path, enable_dbt_projects_profiles_file
    ):
        """
        What: With the flag on but no profiles file present, nothing resolves.
        How: Use an empty dir; enable the flag; call the resolver.
        Expected: resolver returns None.
        """
        assert (
            DBTManager._resolve_profiles_filename(project_path) is None  # noqa: SLF001
        )

    # --- _prepare_profiles_file (staging) ---

    def test_prepare_stages_dbt_projects_file_under_own_name_when_both_present(
        self, tmp_path_factory, profile, enable_dbt_projects_profiles_file
    ):
        """
        What: When both files exist, staging copies dbt_projects_profiles.yml into
              the staging root under its own name (not renamed to profiles.yml).
        How: Write both files into a profiles dir; enable the flag; run
             _prepare_profiles_file into a fresh tmp dir.
        Expected: tmp dir contains dbt_projects_profiles.yml with the parsed
                  content, and _prepare did not create a profiles.yml of its own.
        """
        profiles_dir = tmp_path_factory.mktemp("profiles")
        tmp_dir = tmp_path_factory.mktemp("stage")
        _write_profile(profiles_dir, PROFILES_FILENAME, {"other_profile": {}})
        _write_profile(profiles_dir, DBT_PROJECTS_PROFILES_FILENAME, profile)

        DBTManager._prepare_profiles_file(profiles_dir, tmp_dir)  # noqa: SLF001

        staged = tmp_dir / DBT_PROJECTS_PROFILES_FILENAME
        assert staged.exists()
        assert not (tmp_dir / PROFILES_FILENAME).exists()
        with open(staged) as fp:
            assert yaml.safe_load(fp) == profile

    def test_prepare_leaves_existing_tmp_profiles_yml_untouched(
        self, tmp_path_factory, profile, enable_dbt_projects_profiles_file
    ):
        """
        What: Staging dbt_projects_profiles.yml does not disturb a profiles.yml
              already copied into the staging root from the source project.
        How: Pre-create a profiles.yml in the tmp/staging dir (as copy_to_tmp_dir
             would); write both files in the profiles dir; enable the flag; run
             _prepare_profiles_file.
        Expected: the pre-existing tmp profiles.yml is unchanged and
                  dbt_projects_profiles.yml is staged alongside it.
        """
        profiles_dir = tmp_path_factory.mktemp("profiles")
        tmp_dir = tmp_path_factory.mktemp("stage")
        preexisting = _write_profile(
            tmp_dir, PROFILES_FILENAME, {"copied_from_source": {}}
        )
        original_text = preexisting.read_text()
        _write_profile(profiles_dir, PROFILES_FILENAME, {"other_profile": {}})
        _write_profile(profiles_dir, DBT_PROJECTS_PROFILES_FILENAME, profile)

        DBTManager._prepare_profiles_file(profiles_dir, tmp_dir)  # noqa: SLF001

        assert (tmp_dir / DBT_PROJECTS_PROFILES_FILENAME).exists()
        assert preexisting.read_text() == original_text

    # --- _validate_profiles ---

    def test_validate_accepts_dbt_projects_file(
        self,
        mock_validate_role,
        project_path,
        profile,
        enable_dbt_projects_profiles_file,
    ):
        """
        What: Validation works against dbt_projects_profiles.yml (identical schema
              to profiles.yml).
        How: Write only dbt_projects_profiles.yml with a valid 'dev' profile;
             enable the flag; call _validate_profiles.
        Expected: no error is raised.
        """
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, profile)

        DBTManager()._validate_profiles(SecurePath(project_path), "dev")  # noqa: SLF001

    def test_validate_prefers_dbt_projects_file_when_both_present(
        self,
        mock_validate_role,
        project_path,
        profile,
        enable_dbt_projects_profiles_file,
    ):
        """
        What: Validation reads the higher-precedence dbt_projects_profiles.yml,
              not profiles.yml, when both exist.
        How: Put a profiles.yml that does NOT define the requested profile, and a
             dbt_projects_profiles.yml that DOES; enable the flag; validate 'dev'.
        Expected: validation passes — proving it read dbt_projects_profiles.yml.
                  (If it had read profiles.yml it would raise 'not defined'.)
        """
        _write_profile(
            project_path, PROFILES_FILENAME, {"wrong_profile": {"outputs": {}}}
        )
        _write_profile(project_path, DBT_PROJECTS_PROFILES_FILENAME, profile)

        DBTManager()._validate_profiles(SecurePath(project_path), "dev")  # noqa: SLF001

    def test_validate_raises_with_both_names_when_neither_present(
        self, mock_validate_role, project_path, enable_dbt_projects_profiles_file
    ):
        """
        What: With the flag on and no profiles file present, the error names both
              accepted filenames.
        How: Use an empty dir; enable the flag; call _validate_profiles.
        Expected: CliError mentioning both dbt_projects_profiles.yml and
                  profiles.yml.
        """
        with pytest.raises(CliError) as exc_info:
            DBTManager()._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        assert (
            exc_info.value.message
            == f"{DBT_PROJECTS_PROFILES_FILENAME} or {PROFILES_FILENAME} does not exist "
            f"in directory {project_path.absolute()}."
        )
