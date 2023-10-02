from pathlib import Path
import pytest
from unittest import mock
from textwrap import dedent

from snowcli.cli.nativeapp.manager import (
    NativeAppManager,
    MissingPackageScriptError,
    InvalidPackageScriptError,
)

from tests.project.fixtures import *
from tests.testing_utils.fixtures import *

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = (
    f"{NATIVEAPP_MODULE}.NativeAppManager._execute_queries"
)


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_package_scripts(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = NativeAppManager(str(working_dir))
    native_app_manager._apply_package_scripts()
    assert mock_execute.mock_calls == [
        mock.call(
            dedent(
                f"""\
                    -- package script (1/2)

                    create schema if not exists myapp_pkg_polly.my_shared_content;
                    grant usage on schema myapp_pkg_polly.my_shared_content
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
        mock.call(
            dedent(
                f"""\
                    -- package script (2/2)

                    create or replace table myapp_pkg_polly.my_shared_content.shared_table (
                      col1 number,
                      col2 varchar
                    );
                    grant select on table myapp_pkg_polly.my_shared_content.shared_table
                      to share in application package myapp_pkg_polly;
                """
            )
        ),
    ]


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_missing_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = NativeAppManager(str(working_dir))
    with pytest.raises(MissingPackageScriptError):
        (working_dir / "002-shared.sql").unlink()
        native_app_manager._apply_package_scripts()

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_invalid_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = NativeAppManager(str(working_dir))
    with pytest.raises(InvalidPackageScriptError):
        second_file = working_dir / "002-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ package_name")
        native_app_manager._apply_package_scripts()

    # even though the second script was the one missing, nothing should be executed
    assert mock_execute.mock_calls == []


@mock.patch(NATIVEAPP_MANAGER_EXECUTE_QUERIES)
@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_undefined_var_package_script(mock_execute, project_definition_files):
    working_dir: Path = project_definition_files[0].parent
    native_app_manager = NativeAppManager(str(working_dir))
    with pytest.raises(InvalidPackageScriptError):
        second_file = working_dir / "001-shared.sql"
        second_file.unlink()
        second_file.write_text("select * from {{ abc }}")
        native_app_manager._apply_package_scripts()

    assert mock_execute.mock_calls == []
