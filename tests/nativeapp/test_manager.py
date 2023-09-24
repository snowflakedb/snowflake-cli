import os
import pytest
from textwrap import dedent
from unittest import mock

from snowcli.cli.nativeapp.manager import NativeAppManager
from snowcli.cli.stage.diff import DiffResult
from snowflake.connector.cursor import DictCursor


from tests.testing_utils.fixtures import *

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MODULE}.NativeAppManager._execute_query"

mock_snowflake_yml_file = dedent(
    """\
        definition_version: 1
        native_app:
            name: myapp

            source_stage:
                app_src.stage

            artifacts:
                - setup.sql
                - app/README.md
                - src: app/streamlit/*.py
                  dest: ui/

            package:
                scripts: package/*.sql
    """
)

mock_project_definition_override = {
    "native_app": {
        "application": {
            "name": "sample_application_name",
            "role": "sample_application_role",
        },
        "package": {
            "name": "sample_package_name",
            "role": "sample_package_role",
        },
    }
}


@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{NATIVEAPP_MODULE}.stage_diff")
@mock.patch(f"{NATIVEAPP_MODULE}.sync_local_diff_with_stage")
def test_sync_deploy_root_with_stage(
    mock_local_diff_with_stage, mock_stage_diff, mock_execute, temp_dir, mock_cursor
):
    mock_execute.return_value = mock_cursor([{"CURRENT_ROLE()": "old_role"}], [])
    mock_diff_result = DiffResult()
    mock_stage_diff.return_value = mock_diff_result
    mock_local_diff_with_stage.return_value = None
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    native_app_manager = NativeAppManager()
    native_app_manager.sync_deploy_root_with_stage("new_role", "app_pkg")

    expected = [
        mock.call("select current_role()", cursor_class=DictCursor),
        mock.call("use role new_role"),
        mock.call(f"create schema if not exists app_pkg.app_src"),
        mock.call(
            f"""
                    create stage if not exists app_pkg.app_src.stage
                    encryption = (TYPE = 'SNOWFLAKE_SSE')"""
        ),
        mock.call("use role old_role"),
    ]
    assert mock_execute.mock_calls == expected
    mock_stage_diff.assert_called_once_with(
        native_app_manager.deploy_root, "app_pkg.app_src.stage"
    )
    mock_local_diff_with_stage.assert_called_once_with(
        role="new_role",
        deploy_root_path=native_app_manager.deploy_root,
        diff_result=mock_diff_result,
        stage_path="app_pkg.app_src.stage",
    )


# @mock.patch(NATIVEAPP_MANAGER_EXECUTE)
# def test_drop_object(mock_execute, temp_dir, mock_cursor):
#     mock_execute.return_value = [  # We can re-use this same output for both show and drop since drop is inconsequential
#         mock_cursor(["row1"], []),
#         mock_cursor(
#             [
#                 (
#                     "sample_package_name",
#                     "sample_package_role",
#                     "blank",
#                     "blank",
#                     "blank",
#                     "blank",
#                     "GENERATED_BY_SNOWCLI",
#                 )
#             ],
#             ["name", "owner", "blank", "blank", "blank", "blank", "comment"],
#         ),
#     ]
#     current_working_directory = os.getcwd()
#     create_named_file(
#         file_name="snowflake.yml",
#         dir=current_working_directory,
#         contents=[mock_snowflake_yml_file],
#     )
#     native_app_manager = NativeAppManager()
#     native_app_manager.drop_object(
#         generated_local_yml_overrides=mock_project_definition_override,
#         object_type="package",
#         query_dict={
#             "show": "show application packages like",
#             "drop": "drop application package",
#         },
#     )
#     expected = [
#         call(
#             "sample_package_role", "show application packages like sample_package_name"
#         ),
#         call("sample_package_role", "drop application package sample_package_name"),
#     ]
#     mock_execute.mock_calls == expected


# @mock.patch(NATIVEAPP_MANAGER_EXECUTE)
# def test_drop_object_no_show_object(mock_execute, temp_dir, mock_cursor):
#     mock_execute.return_value = [  # We can re-use this same output for both show and drop since drop is inconsequential
#         mock_cursor(["row1"], []),
#         mock_cursor([], []),
#     ]
#     current_working_directory = os.getcwd()
#     create_named_file(
#         file_name="snowflake.yml",
#         dir=current_working_directory,
#         contents=[mock_snowflake_yml_file],
#     )
#     native_app_manager = NativeAppManager()
#     with pytest.raises(
#         CouldNotDropObjectError,
#         match="Role sample_package_role does not own any application package with name sample_package_name!",
#     ):
#         native_app_manager.drop_object(
#             generated_local_yml_overrides=mock_project_definition_override,
#             object_type="package",
#             query_dict={
#                 "show": "show application packages like",
#                 "drop": "drop application package",
#             },
#         )


# @mock.patch(NATIVEAPP_MANAGER_EXECUTE)
# def test_drop_object_no_special_comment(mock_execute, temp_dir, mock_cursor):
#     mock_execute.return_value = [  # We can re-use this same output for both show and drop since drop is inconsequential
#         mock_cursor(["row1"], []),
#         mock_cursor(
#             [
#                 (
#                     "sample_package_name",
#                     "sample_package_role",
#                     "blank",
#                     "blank",
#                     "blank",
#                     "blank",
#                     "not_generated_by_snowcli",
#                 )
#             ],
#             ["name", "owner", "blank", "blank", "blank", "blank", "comment"],
#         ),
#     ]
#     current_working_directory = os.getcwd()
#     create_named_file(
#         file_name="snowflake.yml",
#         dir=current_working_directory,
#         contents=[mock_snowflake_yml_file],
#     )
#     native_app_manager = NativeAppManager()
#     with pytest.raises(
#         CouldNotDropObjectError,
#         match="Application Package sample_package_name was not created by SnowCLI. Cannot drop the application package.",
#     ):
#         native_app_manager.drop_object(
#             generated_local_yml_overrides=mock_project_definition_override,
#             object_type="package",
#             query_dict={
#                 "show": "show application packages like",
#                 "drop": "drop application package",
#             },
#         )
