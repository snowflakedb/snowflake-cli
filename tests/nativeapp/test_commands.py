import pytest
from unittest import mock

from snowcli.cli.nativeapp.init import InitError

from tests.testing_utils.fixtures import *

PROJECT_NAME = "demo_na_project"


@mock.patch(
    "snowcli.cli.nativeapp.init._init_without_user_provided_template",
    side_effect=InitError(),
)
def test_init_no_template_raised_exception(
    mock_init_without_user_provided_template, runner, temp_dir
):
    with pytest.raises(InitError):
        # temp_dir will be cwd for the rest of this test
        result = runner.invoke(["app", "init", PROJECT_NAME])

        assert result.exit_code == 1


@mock.patch(
    "snowcli.cli.nativeapp.init._init_without_user_provided_template",
    return_value=None,
)
def test_init_no_template_success(
    mock_init_without_user_provided_template, runner, temp_dir, snapshot
):
    # temp_dir will be cwd for the rest of this test
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert result.exit_code == 0
    assert result.output == snapshot
