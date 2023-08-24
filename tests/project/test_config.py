import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from snowcli.cli.project.config import load_project_config, load_local_config

from tests.project.fixtures import *

def test_project_1_config(with_project_1):
    print(with_project_1)
    [project_yml, local_yml] = with_project_1
    project = load_project_config(project_yml)
    local = load_local_config(local_yml)
    assert project["native_app"]["name"] == "myapp"
    assert local["application"]["role"] == "accountadmin"
    assert local["application"]["debug"] == True
