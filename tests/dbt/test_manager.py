from textwrap import dedent

import pytest
import yaml
from snowflake.cli._plugins.dbt.manager import DBTManager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath


class TestDeploy:
    @pytest.fixture()
    def profile(self):
        return {
            "dev": {
                "outputs": {
                    "local": {
                        "account": "test_account",
                        "database": "testdb",
                        "role": "test_role",
                        "schema": "test_schema",
                        "threads": 4,
                        "type": "snowflake",
                        "user": "test_user",
                        "warehouse": "test_warehouse",
                    }
                }
            }
        }

    @pytest.fixture
    def project_path(self, tmp_path_factory):
        source_path = tmp_path_factory.mktemp("dbt_project")
        yield source_path

    def _generate_profile(self, project_path, profile):
        dbt_profiles_file = project_path / "profiles.yml"
        dbt_profiles_file.write_text(yaml.dump(profile))

    def test_validate_profiles_raises_when_file_does_not_exist(self, project_path):

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        assert (
            exc_info.value.message
            == f"profiles.yml does not exist in directory {project_path.absolute()}."
        )

    def test_validate_profiles_raises_when_profile_is_not_in_the_file(
        self, project_path, profile
    ):
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "another_profile_name"
            )

        assert (
            exc_info.value.message
            == "profile another_profile_name is not defined in profiles.yml"
        )

    def test_validate_profiles_raises_when_extra_profiles_are_defined(
        self, project_path, profile
    ):
        profile["another_profile"] = {}
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        expected_error_message = """Found following errors in profiles.yml. Please fix them before proceeding:
another_profile
 * Remove unnecessary profiles"""
        assert exc_info.value.message == dedent(expected_error_message)

    def test_validate_profiles_raises_when_required_fields_are_missing(
        self, project_path, profile
    ):
        profile["dev"]["outputs"]["local"].pop("warehouse", None)
        profile["dev"]["outputs"]["local"].pop("role", None)
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        expected_error_message = """Found following errors in profiles.yml. Please fix them before proceeding:
dev
 * Missing required fields: role, warehouse in target local"""
        assert exc_info.value.message == dedent(expected_error_message)

    def test_validate_profiles_raises_when_unsupported_fields_are_provided(
        self, project_path, profile
    ):
        profile["dev"]["outputs"]["local"]["password"] = "very secret password"
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        expected_error_message = """Found following errors in profiles.yml. Please fix them before proceeding:
dev
 * Unsupported fields found: password in target local"""
        assert exc_info.value.message == dedent(expected_error_message)
        assert "very secret password" not in exc_info.value.message

    def test_validate_profiles_raises_when_type_is_wrong(self, project_path, profile):
        profile["dev"]["outputs"]["local"]["type"] = "sqlite"
        self._generate_profile(project_path, profile)

        with pytest.raises(CliError) as exc_info:
            DBTManager._validate_profiles(  # noqa: SLF001
                SecurePath(project_path), "dev"
            )

        expected_error_message = """Found following errors in profiles.yml. Please fix them before proceeding:
dev
 * Value for type field is invalid. Should be set to `snowflake` in target local"""
        assert exc_info.value.message == dedent(expected_error_message)
