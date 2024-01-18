import unittest

from snowflake.cli.plugins.nativeapp.constants import SPECIAL_COMMENT
from snowflake.cli.plugins.nativeapp.exceptions import (
    CouldNotDropApplicationPackageWithVersions,
    UnexpectedOwnerError,
)
from snowflake.cli.plugins.nativeapp.manager import SnowflakeSQLExecutionError
from snowflake.cli.plugins.nativeapp.teardown_processor import (
    NativeAppTeardownProcessor,
)
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor

from tests.nativeapp.patch_utils import mock_get_app_pkg_distribution_in_sf
from tests.nativeapp.utils import *
from tests.testing_utils.fixtures import *


def _get_na_teardown_processor():
    dm = DefinitionManager()
    return NativeAppTeardownProcessor(
        project_definition=dm.project_definition["native_app"],
        project_root=dm.project_root,
    )


# Test drop_generic_object() with success
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_generic_object_success(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("drop application myapp")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_generic_object(
        object_type="application", object_name="myapp", role="app_role"
    )
    assert mock_execute.mock_calls == expected


# Test drop_generic_object() with an exception
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
def test_drop_generic_object_failure_w_exception(mock_execute, temp_dir, mock_cursor):
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                ProgrammingError(
                    msg="Object does not exist, or operation cannot be performed.",
                    errno=2043,
                ),
                mock.call("drop application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    with pytest.raises(SnowflakeSQLExecutionError):
        teardown_processor.drop_generic_object(
            object_type="application package",
            object_name="app_pkg",
            role="package_role",
        )
    assert mock_execute.mock_calls == expected


# Test drop_application() when no application exists
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO, return_value=None)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_no_existing_application(
    mock_log_info, mock_get_existing_app_info, auto_yes_param, temp_dir
):
    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()
    mock_log_info.assert_called_once_with(
        "Role app_role does not own any application with the name myapp, or the application does not exist."
    )


# Test drop_application() when it has a different owner role
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_incorrect_owner(
    mock_get_existing_app_info, auto_yes_param, temp_dir
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "different_owner",
        "comment": "some_comment",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    with pytest.raises(UnexpectedOwnerError):
        teardown_processor.drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()


# Test drop_application() successfully when it has special comment
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_has_special_comment(
    mock_drop_generic_object,
    mock_is_correct_owner,
    mock_get_existing_app_info,
    auto_yes_param,
    temp_dir,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": SPECIAL_COMMENT,
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()
    mock_is_correct_owner.assert_called_once()
    mock_drop_generic_object.assert_called_once()


# Test drop_application() successfully when it has special comment but is a quoted string
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_application_has_special_comment_and_quoted_name(
    mock_execute,
    auto_yes_param,
    temp_dir,
    mock_cursor,
):
    side_effects, expected = mock_execute_helper(
        [
            # Show apps
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "My Application",
                            "comment": SPECIAL_COMMENT,
                            "version": "UNVERSIONED",
                            "owner": "APP_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show applications like 'My Application'", cursor_class=DictCursor
                ),
            ),
            (None, mock.call("use role old_role")),
            # Drop app
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call('drop application "My Application"')),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir=current_working_directory,
        contents=[quoted_override_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes_param)
    mock_execute.mock_calls == expected


# Test drop_application() without special comment AND auto_yes is False AND should_drop is False
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=False)
@mock.patch(f"{TEARDOWN_MODULE}.print")
def test_drop_application_user_prohibits_drop(
    mock_log_info,
    mock_confirm,
    mock_drop_generic_object,
    mock_is_correct_owner,
    mock_get_existing_app_info,
    temp_dir,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": "no_special_comment",
        "created_on": "dummy",
        "source": "dummy",
        "version": "dummy",
        "patch": "dummy",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes=False)
    mock_get_existing_app_info.assert_called_once()
    mock_is_correct_owner.assert_called_once()
    mock_drop_generic_object.assert_not_called()
    mock_log_info.assert_called_once_with("Did not drop application myapp.")


# Test drop_application() without special comment AND auto_yes is False AND should_drop is True
# Test drop_application() without special comment AND auto_yes is True
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=True)
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],
)
def test_drop_application_user_allows_drop(
    mock_confirm,
    mock_execute,
    mock_drop_generic_object,
    mock_is_correct_owner,
    mock_get_existing_app_info,
    auto_yes_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_info.return_value = {
        "name": "myapp",
        "owner": "app_role",
        "comment": "no_special_comment",
        "created_on": "dummy",
        "source": "dummy",
        "version": "dummy",
        "patch": "dummy",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes_param)
    mock_get_existing_app_info.assert_called_once()
    mock_is_correct_owner.assert_called_once()
    mock_drop_generic_object.assert_called_once()
    mock_execute.mock_calls == expected


# Test idempotent drop_application()
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],  # This should have no effect on the test
)
def test_drop_application_idempotent(
    mock_drop_generic_object,
    mock_is_correct_owner,
    mock_get_existing_app_info,
    auto_yes_param,
    temp_dir,
):
    side_effects_for_get_existing_app_info = [
        {"name": "myapp", "owner": "app_role", "comment": SPECIAL_COMMENT},
        None,
        None,
    ]
    mock_get_existing_app_info.side_effect = side_effects_for_get_existing_app_info

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_application(auto_yes_param)
    teardown_processor.drop_application(auto_yes_param)
    teardown_processor.drop_application(auto_yes_param)

    mock_get_existing_app_info.call_count == 3
    mock_is_correct_owner.assert_called_once()
    mock_drop_generic_object.assert_called_once()


# Test drop_package() when no application package exists
@mock.patch(
    TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO,
    return_value=None,
)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_package_no_existing_application(
    mock_log_info, mock_get_existing_app_pkg_info, auto_yes_param, temp_dir
):

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    mock_get_existing_app_pkg_info.assert_called_once()
    mock_log_info.assert_called_once_with(
        "Role package_role does not own any application package with the name app_pkg, or the package does not exist."
    )


# Test drop_package() when it has a different owner role
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_drop_package_incorrect_owner(
    mock_get_existing_app_pkg_info, auto_yes_param, temp_dir
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "different_owner",
        "comment": "some_comment",
    }

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    with pytest.raises(UnexpectedOwnerError):
        teardown_processor.drop_package(auto_yes_param)
    mock_get_existing_app_pkg_info.assert_called_once()


# Test drop_package when the package has more than 0 existing versions
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@pytest.mark.parametrize(
    "auto_yes_param",
    [True, False],  # This should have no effect on the test
)
def test_show_versions_failure_w_exception(
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    temp_dir,
    mock_cursor,
):
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "different_owner",
        "comment": "some_comment",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role app_role")),
            (
                mock_cursor([("row1"), ("row2")], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    with pytest.raises(CouldNotDropApplicationPackageWithVersions):
        teardown_processor.drop_package(auto_yes_param)
    mock_is_correct_owner.assert_called_once()
    mock_get_existing_app_pkg_info.assert_called_once()


# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is False
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME, return_value=True)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=False)
def test_drop_package_no_mismatch_no_drop(
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_log_info,
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    temp_dir,
    mock_cursor,
):
    mock_get_distribution.return_value = "external"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "some_comment",
        "created_on": "dummy",
        "distribution": "external",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes=False)
    mock_execute.mock_calls == expected
    mock_log_info.assert_any_call("Did not drop application package app_pkg.")


# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is True
# Test drop_package when there is no distribution mismatch AND distribution = external AND auto_yes is True
# Test drop_package when there is distribution mismatch AND distribution = external AND auto_yes is False AND should_drop is True
# Test drop_package when there is distribution mismatch AND distribution = external AND auto_yes is True
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",
    [(False, True), (False, False), (True, True), (True, False)],
)
def test_drop_package_variable_mismatch_allowed_user_allows_drop(
    mock_drop_generic_object,
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_log_warning,
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temp_dir,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "external"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "some_comment",
        "created_on": "dummy",
        "distribution": "external",
    }
    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    mock_execute.mock_calls == expected
    if not is_pkg_distribution_same:
        mock_log_warning.assert_any_call(
            "Continuing to execute `snow app teardown` on app pkg app_pkg with distribution external."
        )
    if not auto_yes_param:
        mock_log_warning.assert_any_call(
            "Application package app_pkg in your Snowflake account has distribution property 'external'"
        )
    mock_drop_generic_object.assert_called_once()


# Test drop_package when there is no distribution mismatch AND distribution = internal AND special comment is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND special comment is True
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",  # auto_yes_param should have no effect on the test
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_drop_package_variable_mistmatch_w_special_comment_auto_drop(
    mock_log_warning,
    mock_drop_generic_object,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temp_dir,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": SPECIAL_COMMENT,
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    mock_execute.mock_calls == expected
    mock_drop_generic_object.assert_called_once()
    if not is_pkg_distribution_same:
        mock_log_warning.assert_any_call(
            "Continuing to execute `snow app teardown` on app pkg app_pkg with distribution internal."
        )


# Test drop_package when there is no distribution mismatch AND distribution = internal AND special comment is True AND name is quoted
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@pytest.mark.parametrize(
    "auto_yes_param", [True, False]  # auto_yes_param should have no effect on the test
)
def test_drop_package_variable_mistmatch_w_special_comment_quoted_name_auto_drop(
    mock_get_distribution, mock_execute, auto_yes_param, temp_dir, mock_cursor
):
    mock_get_distribution.return_value = "internal"

    side_effects, expected = mock_execute_helper(
        [
            # Show app pkg
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor(
                    [
                        {
                            "name": "My Package",
                            "comment": SPECIAL_COMMENT,
                            "owner": "PACKAGE_ROLE",
                        }
                    ],
                    [],
                ),
                mock.call(
                    "show application packages like 'My Package'",
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            # Show versions
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call(
                    'show versions in application package "My Package"',
                    cursor_class=DictCursor,
                ),
            ),
            (None, mock.call("use role old_role")),
            # Drop app pkg
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (None, mock.call('drop application package "My Package"')),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )
    create_named_file(
        file_name="snowflake.local.yml",
        dir=current_working_directory,
        contents=[quoted_override_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    mock_execute.mock_calls == expected


# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is False
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is False
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=False)
@mock.patch(f"{TEARDOWN_MODULE}.print")
@pytest.mark.parametrize("is_pkg_distribution_same", [True, False])
def test_drop_package_variable_mistmatch_no_special_comment_user_prohibits_drop(
    mock_log_warning,
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    is_pkg_distribution_same,
    temp_dir,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "dummy",
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes=False)
    mock_execute.mock_calls == expected
    mock_log_warning.assert_any_call("Did not drop application package app_pkg.")
    if not is_pkg_distribution_same:
        mock_log_warning.assert_any_call(
            "Continuing to execute `snow app teardown` on app pkg app_pkg with distribution internal."
        )


# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is True
# Test drop_package when there is no distribution mismatch AND distribution = internal AND without special comment AND auto_yes is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is False AND should_drop is True
# Test drop_package when there is distribution mismatch AND distribution = internal AND without special comment AND auto_yes is True
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME)
@mock.patch(f"{TEARDOWN_MODULE}.{TYPER_CONFIRM}", return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@pytest.mark.parametrize(
    "auto_yes_param, is_pkg_distribution_same",  # auto_yes_param should have no effect on the test
    [(True, True), (True, False), (False, True), (False, False)],
)
def test_drop_package_variable_mistmatch_no_special_comment_user_allows_drop(
    mock_drop_generic_object,
    mock_confirm,
    mock_is_distribution_same,
    mock_get_distribution,
    mock_execute,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    is_pkg_distribution_same,
    temp_dir,
    mock_cursor,
):
    mock_is_distribution_same.return_value = is_pkg_distribution_same
    mock_get_distribution.return_value = "internal"
    mock_get_existing_app_pkg_info.return_value = {
        "name": "app_pkg",
        "owner": "package_role",
        "comment": "dummy",
        "created_on": "dummy",
        "distribution": "internal",
    }

    side_effects, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    mock_execute.mock_calls == expected
    mock_drop_generic_object.assert_called_once()


# Test idempotent drop_package()
@mock.patch(TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO)
@mock.patch(TEARDOWN_PROCESSOR_IS_CORRECT_OWNER, return_value=True)
@mock.patch(TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT, return_value=None)
@mock.patch(NATIVEAPP_MANAGER_EXECUTE)
@mock_get_app_pkg_distribution_in_sf()
@mock.patch(NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME, return_value=True)
@pytest.mark.parametrize(
    "auto_yes_param",
    [False, True],  # This should have no effect on the test
)
def test_drop_package_idempotent(
    mock_is_distribution_same,
    mock_get_app_pkg_distribution,
    mock_execute,
    mock_drop_generic_object,
    mock_is_correct_owner,
    mock_get_existing_app_pkg_info,
    auto_yes_param,
    temp_dir,
    mock_cursor,
):
    mock_get_app_pkg_distribution.return_value = "internal"
    side_effects_for_execute, expected = mock_execute_helper(
        [
            (
                mock_cursor([{"CURRENT_ROLE()": "old_role"}], []),
                mock.call("select current_role()", cursor_class=DictCursor),
            ),
            (None, mock.call("use role package_role")),
            (
                mock_cursor([], []),
                mock.call("show versions in application package app_pkg"),
            ),
            (None, mock.call("use role old_role")),
        ]
    )
    mock_execute.side_effect = side_effects_for_execute

    side_effects_for_get_existing_app_pkg_info = [
        {"name": "app_pkg", "owner": "package_role", "comment": SPECIAL_COMMENT},
        None,
        None,
    ]
    mock_get_existing_app_pkg_info.side_effect = (
        side_effects_for_get_existing_app_pkg_info
    )

    current_working_directory = os.getcwd()
    create_named_file(
        file_name="snowflake.yml",
        dir=current_working_directory,
        contents=[mock_snowflake_yml_file],
    )

    teardown_processor = _get_na_teardown_processor()
    teardown_processor.drop_package(auto_yes_param)
    teardown_processor.drop_package(auto_yes_param)
    teardown_processor.drop_package(auto_yes_param)

    mock_get_existing_app_pkg_info.call_count == 3
    mock_is_correct_owner.assert_called_once()
    mock_drop_generic_object.assert_called_once()
    mock_execute.mock_calls == expected
