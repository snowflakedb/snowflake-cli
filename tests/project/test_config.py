import pytest
from unittest import mock
from tests.project.fixtures import *
from tests.testing_utils.fixtures import *

from snowcli.config import CliConfigManager, get_default_connection

from snowcli.cli.project.config import (
    load_project_config,
    load_local_config,
    generate_local_config,
)


@pytest.mark.parametrize("project_context", ["project_1"], indirect=True)
def test_na_project_1(project_context):
    [project_yml, local_yml] = project_context
    project = load_project_config(project_yml)
    local = load_local_config(local_yml)
    assert project["native_app"]["name"] == "myapp"
    assert project["native_app"]["deploy_root"] == "output/deploy/"
    assert local["native_app"]["package"]["role"] == "accountadmin"
    assert local["native_app"]["application"]["debug"] == True


@pytest.mark.parametrize("project_context", ["project_1"], indirect=True)
def test_build_local_from_project(test_snowcli_config, project_context):
    [project_yml, _] = project_context
    project = load_project_config(project_yml)

    assert project["native_app"]["name"] == "myapp"

    cm = CliConfigManager(file_path=test_snowcli_config)
    cm.read_config()
    conn = cm.get_connection(get_default_connection())

    assert conn["role"] == "test_role"

    from os import getenv as original_getenv

    def mock_getenv(key: str) -> str | None:
        if key.lower() == "user":
            return "jsmith"
        return original_getenv(key)

    with mock.patch("os.getenv", side_effect=mock_getenv):
        local = generate_local_config(project, conn)
        assert local["native_app"]["application"]["name"] == "myapp_jsmith"
        assert local["native_app"]["application"]["role"] == "test_role"
        assert local["native_app"]["package"]["name"] == "myapp_pkg_jsmith"
        assert local["native_app"]["package"]["role"] == "test_role"
