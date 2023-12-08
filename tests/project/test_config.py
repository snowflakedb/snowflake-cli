from unittest import mock
from unittest.mock import PropertyMock

import pytest
from snowcli.cli.project.definition import (
    load_project_definition,
)
from strictyaml import YAMLValidationError

from tests.project.fixtures import *
from tests.testing_utils.fixtures import *


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert project["native_app"]["name"] == "myapp"
    assert project["native_app"]["deploy_root"] == "output/deploy/"
    assert project["native_app"]["package"]["role"] == "accountadmin"
    assert project["native_app"]["application"]["name"] == "myapp_polly"
    assert project["native_app"]["application"]["role"] == "myapp_consumer"
    assert project["native_app"]["application"]["debug"] == True


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
def test_na_minimal_project(project_definition_files: List[Path]):
    project = load_project_definition(project_definition_files)
    assert project["native_app"]["name"] == "minimal"
    assert project["native_app"]["artifacts"] == ["setup.sql", "README.md"]


@pytest.mark.parametrize("project_definition_files", ["underspecified"], indirect=True)
def test_underspecified_project(project_definition_files):
    with pytest.raises(YAMLValidationError) as exc_info:
        load_project_definition(project_definition_files)

    assert "required key(s) 'artifacts' not found" in str(exc_info.value)


@pytest.mark.parametrize(
    "project_definition_files", ["no_definition_version"], indirect=True
)
def test_fails_without_definition_version(project_definition_files):
    with pytest.raises(YAMLValidationError) as exc_info:
        load_project_definition(project_definition_files)

    assert "required key(s) 'definition_version' not found" in str(exc_info.value)


@pytest.mark.parametrize("project_definition_files", ["unknown_fields"], indirect=True)
def test_accepts_unknown_fields(project_definition_files):
    project = load_project_definition(project_definition_files)
    assert project["native_app"]["name"] == "unknown_fields"
    assert project["native_app"]["unknown_fields_accepted"] == True
