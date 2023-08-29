import pytest
from typing import Optional, List
from unittest import mock
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *

from strictyaml import YAMLValidationError

from snowcli.cli.project.config import load_project_config, generate_local_override_yml


@pytest.mark.parametrize("project_config_files", ["project_1"], indirect=True)
def test_na_project_1(project_config_files):
    project = load_project_config(project_config_files)
    assert project["native_app"]["name"] == "myapp"
    assert project["native_app"]["deploy_root"] == "output/deploy/"
    assert project["native_app"]["package"]["role"] == "accountadmin"
    assert project["native_app"]["application"]["name"] == "myapp_polly"
    assert project["native_app"]["application"]["role"] == "myapp_consumer"
    assert project["native_app"]["application"]["debug"] == True


@pytest.mark.parametrize("project_config_files", ["minimal"], indirect=True)
def test_na_minimal_project(
    project_config_files: List[Path], mock_cursor, test_snowcli_config
):
    project = load_project_config(project_config_files)
    assert project["native_app"]["name"] == "minimal"
    assert project["native_app"]["package"]["scripts"] == "package/*.sql"
    assert project["native_app"]["artifacts"] == ["setup.sql", "README.md"]

    from os import getenv as original_getenv

    def mock_getenv(key: str, default: Optional[str] = None) -> str | None:
        if key.lower() == "user":
            return "jsmith"
        return original_getenv(key, default)

    def mock_execute_string(query) -> SnowflakeCursor:
        if query == "select current_role()":
            return mock_cursor(
                rows=[("resolved_role",)],
                columns=["CURRENT_ROLE()"],
            )
        elif query == "select current_warehouse()":
            return mock_cursor(
                rows=[("resolved_warehouse",)],
                columns=["CURRENT_WAREHOUSE()"],
            )

    with mock.patch(
        "snowcli.cli.common.snow_cli_global_context.SnowCliGlobalContextManager.execute_string",
        side_effect=mock_execute_string,
    ):
        with mock.patch("os.getenv", side_effect=mock_getenv):
            # probably a better way of going about this is to not generate
            # a config structure for these values but directly return defaults
            # in "getter" function (higher-level data structures).
            local = generate_local_override_yml(project)
            assert local["native_app"]["application"]["name"] == "minimal_jsmith"
            assert local["native_app"]["application"]["role"] == "resolved_role"
            assert (
                local["native_app"]["application"]["warehouse"] == "resolved_warehouse"
            )
            assert local["native_app"]["application"]["debug"] == True
            assert local["native_app"]["package"]["name"] == "minimal_pkg_jsmith"
            assert local["native_app"]["package"]["role"] == "resolved_role"


@pytest.mark.parametrize("project_config_files", ["underspecified"], indirect=True)
def test_underspecified_project(project_config_files):
    with pytest.raises(YAMLValidationError) as exc_info:
        load_project_config(project_config_files)

    assert "required key(s) 'artifacts' not found" in str(exc_info.value)


@pytest.mark.parametrize("project_config_files", ["no_config_version"], indirect=True)
def test_fails_without_config_version(project_config_files):
    with pytest.raises(YAMLValidationError) as exc_info:
        load_project_config(project_config_files)

    assert "required key(s) 'config_version' not found" in str(exc_info.value)


@pytest.mark.parametrize("project_config_files", ["unknown_fields"], indirect=True)
def test_accepts_unknown_fields(project_config_files):
    project = load_project_config(project_config_files)
    assert project["native_app"]["name"] == "unknown_fields"
    assert project["native_app"]["unknown_fields_accepted"] == True
