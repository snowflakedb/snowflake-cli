import os
from pathlib import Path
from textwrap import dedent
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dbt.constants import PROFILES_FILENAME
from snowflake.cli._plugins.dbt.manager import DBTDeployAttributes, DBTManager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath
from snowflake.connector import ProgrammingError


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

        expected_query = f'CREATE DBT PROJECT "MockDaTaBaSe"."PuBlIc"."caseSenSITIVEnAME"\nFROM @TestDB.TestSchema.DBT_PROJECT_caseSenSITIVEnAME_{mock_time()}_STAGE'
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
