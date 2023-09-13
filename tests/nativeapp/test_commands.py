import pytest
from unittest import mock

from tests.testing_utils.fixtures import *

PROJECT_NAME = "demo_na_project"


@mock.patch(
    "snowcli.cli.nativeapp.init._init_with_url_and_template",
    return_value=None,
)
def test_init_no_template_success(
    mock_init_with_url_and_template, runner, temp_dir, snapshot
):
    # temp_dir will be cwd for the rest of this test
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert result.exit_code == 0
    assert result.output == snapshot
