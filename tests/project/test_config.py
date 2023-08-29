import pytest
from typing import Optional
from unittest import mock
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *

from strictyaml import YAMLValidationError

from snowcli.config import CliConfigManager, get_default_connection

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
def test_na_minimal_project(project_config_files, test_snowcli_config):
    project = load_project_config(project_config_files)
    assert project["native_app"]["name"] == "minimal"
    assert project["native_app"]["package"]["scripts"] == "package/*.sql"
    assert project["native_app"]["artifacts"] == ["setup.sql", "README.md"]

    from os import getenv as original_getenv

    def mock_getenv(key: str, default: Optional[str]) -> str | None:
        if key.lower() == "user":
            return "jsmith"
        return original_getenv(key, default)

    # TODO: test that our resolved role is used as default if no role given in connection profile
    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()
    conn = cm.get_connection(get_default_connection())
    assert conn["role"] == "test_role"

    with mock.patch("os.getenv", side_effect=mock_getenv):
        # probably a better way of going about this is to not generate
        # a config structure for these values but directly return defaults
        # in "getter" function (higher-level data structures).
        local = generate_local_override_yml(project, conn)
        assert local["native_app"]["application"]["name"] == "minimal_jsmith"
        assert local["native_app"]["application"]["role"] == "test_role"
        assert local["native_app"]["package"]["name"] == "minimal_pkg_jsmith"
        assert local["native_app"]["package"]["role"] == "test_role"


@pytest.mark.parametrize("project_config_files", ["underspecified"], indirect=True)
def test_underspecified_project(project_config_files):
    with pytest.raises(YAMLValidationError) as exc_info:
        load_project_config(project_config_files)

    assert "required key(s) 'artifacts' not found" in str(exc_info.value)


@pytest.mark.parametrize("project_config_files", ["unknown_fields"], indirect=True)
def test_accepts_unknown_fields(project_config_files):
    project = load_project_config(project_config_files)
    assert project["native_app"]["name"] == "unknown_fields"
    assert project["native_app"]["unknown_fields_accepted"] == True
