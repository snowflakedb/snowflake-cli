from textwrap import dedent
from unittest import mock
from unittest.mock import PropertyMock

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
TEARDOWN_MODULE = "snowcli.cli.nativeapp.teardown_processor"
TYPER_CONFIRM = "typer.confirm"
RUN_MODULE = "snowcli.cli.nativeapp.run_processor"

TEARDOWN_PROCESSOR = f"{TEARDOWN_MODULE}.NativeAppTeardownProcessor"
NATIVEAPP_MANAGER = f"{NATIVEAPP_MODULE}.NativeAppManager"
RUN_PROCESSOR = f"{RUN_MODULE}.NativeAppRunProcessor"

NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MANAGER}._execute_query"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = f"{NATIVEAPP_MANAGER}._execute_queries"
NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF = (
    f"{NATIVEAPP_MANAGER}.get_app_pkg_distribution_in_snowflake"
)
NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME = (
    f"{NATIVEAPP_MANAGER}.is_app_pkg_distribution_same_in_sf"
)
NATIVEAPP_MANAGER_TYPER_CONFIRM = f"{NATIVEAPP_MODULE}.typer.confirm"

TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO = f"{TEARDOWN_PROCESSOR}.get_existing_app_info"
TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO = (
    f"{TEARDOWN_PROCESSOR}.get_existing_app_pkg_info"
)
TEARDOWN_PROCESSOR_IS_CORRECT_OWNER = f"{TEARDOWN_MODULE}.is_correct_owner"
TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT = f"{TEARDOWN_PROCESSOR}.drop_generic_object"

RUN_PROCESSOR_GET_EXISTING_APP_INFO = f"{RUN_PROCESSOR}.get_existing_app_info"
RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO = f"{RUN_PROCESSOR}.get_existing_app_pkg_info"

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

            application:
                name: myapp
                role: app_role
                warehouse: app_warehouse
                debug: true

            package:
                name: app_pkg
                role: package_role
                scripts:
                    - shared_content.sql
    """
)

quoted_override_yml_file = dedent(
    """\
        native_app:
            application:
                name: >-
                    "My Application"
            package:
                name: >-
                    "My Package"
    """
)

mock_connection = mock.patch(
    "snowcli.cli.common.cli_global_context._CliGlobalContextAccess.connection",
    new_callable=PropertyMock,
)

mock_get_app_pkg_distribution_in_sf = mock.patch(
    NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF,
    new_callable=PropertyMock,
)


def mock_execute_helper(mock_input: list):
    side_effects, expected = map(list, zip(*mock_input))
    return side_effects, expected
