from textwrap import dedent
from unittest import mock
from unittest.mock import PropertyMock

NATIVEAPP_MODULE = "snowcli.cli.nativeapp.manager"
NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MODULE}.NativeAppManager._execute_query"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = (
    f"{NATIVEAPP_MODULE}.NativeAppManager._execute_queries"
)
NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF = (
    f"{NATIVEAPP_MODULE}.NativeAppManager.get_app_pkg_distribution_in_snowflake"
)
NATIVEAPP_MANAGER_TYPER_CONFIRM = f"{NATIVEAPP_MODULE}.typer.confirm"

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
