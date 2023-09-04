import contextlib
import os
import pytest
from tempfile import NamedTemporaryFile, TemporaryDirectory
from textwrap import dedent
from unittest import mock
from unittest.mock import call
from subprocess import CalledProcessError

from tests.testing_utils.fixtures import *

PROJECT_NAME = "demo_na_project"


@mock.patch("pathlib.Path.is_file", return_value=True)
def test_init_no_template_w_existing_yml(mock_path_is_file, runner):
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert (
        "Cannot initialize a new project within an existing Native Application project!"
        in result.output
    )
    assert result.exit_code != 0


@mock.patch("pathlib.Path.exists", return_value=True)
def test_init_no_template_w_existing_directory(mock_path_exists, runner):
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert (
        f"This directory already contains a sub-directory called {PROJECT_NAME}. Please try a different name."
        in result.output
    )
    assert result.exit_code != 0


@mock.patch("subprocess.check_output", return_value="git version 2.2")
def test_init_no_template_git_fails(mock_get_client_git_version, runner):
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert (
        "Init requires git version to be at least 2.25.0. Please update git and try again."
        in result.output
    )
    assert result.exit_code != 0


@mock.patch(
    "snowcli.cli.nativeapp.manager._init_without_user_provided_template",
    side_effect=CalledProcessError(1, "Some Mocked Error"),
)
def test_init_no_template_raised_exception(
    mock_init_without_user_provided_template, runner, temp_dir
):
    # temp_dir will be cwd for the rest of this test
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert result.exit_code != 0


@mock.patch(
    "snowcli.cli.nativeapp.manager._init_without_user_provided_template",
    return_value=None,
)
def test_init_no_template_success(
    mock_init_without_user_provided_template, runner, temp_dir, snapshot
):
    # temp_dir will be cwd for the rest of this test
    result = runner.invoke(["app", "init", PROJECT_NAME])

    assert result.exit_code == 0
    assert result.output == snapshot
