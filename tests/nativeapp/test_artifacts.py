# import pytest
# from typing import Optional, List
# from unittest import mock
# from tests.project.fixtures import *
# from tests.testing_utils.fixtures import *

# from strictyaml import YAMLValidationError

# from snowcli.cli.project.definition import (
#     load_project_definition,
# )


# @pytest.mark.parametrize(["project_root", "project_definition_files"], ["project_1"], indirect=True)
# def test_na_project_1(project_definition_files, project_root):
#     project = load_project_definition(project_definition_files)
