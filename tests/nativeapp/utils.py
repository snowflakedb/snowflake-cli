from textwrap import dedent

NATIVEAPP_MODULE = "snowflake.cli.plugins.nativeapp.manager"
TEARDOWN_MODULE = "snowflake.cli.plugins.nativeapp.teardown_processor"
TYPER_CONFIRM = "typer.confirm"
RUN_MODULE = "snowflake.cli.plugins.nativeapp.run_processor"
VERSION_MODULE = "snowflake.cli.plugins.nativeapp.version.version_processor"

TEARDOWN_PROCESSOR = f"{TEARDOWN_MODULE}.NativeAppTeardownProcessor"
NATIVEAPP_MANAGER = f"{NATIVEAPP_MODULE}.NativeAppManager"
RUN_PROCESSOR = f"{RUN_MODULE}.NativeAppRunProcessor"

NATIVEAPP_MANAGER_EXECUTE = f"{NATIVEAPP_MANAGER}._execute_query"
NATIVEAPP_MANAGER_EXECUTE_QUERIES = f"{NATIVEAPP_MANAGER}._execute_queries"
NATIVEAPP_MANAGER_APP_PKG_DISTRIBUTION_IN_SF = (
    f"{NATIVEAPP_MANAGER}.get_app_pkg_distribution_in_snowflake"
)
NATIVEAPP_MANAGER_IS_APP_PKG_DISTRIBUTION_SAME = (
    f"{NATIVEAPP_MANAGER}.verify_project_distribution"
)

TEARDOWN_PROCESSOR_GET_EXISTING_APP_INFO = f"{TEARDOWN_PROCESSOR}.get_existing_app_info"
TEARDOWN_PROCESSOR_GET_EXISTING_APP_PKG_INFO = (
    f"{TEARDOWN_PROCESSOR}.get_existing_app_pkg_info"
)
TEARDOWN_PROCESSOR_IS_CORRECT_OWNER = f"{TEARDOWN_MODULE}.ensure_correct_owner"
TEARDOWN_PROCESSOR_DROP_GENERIC_OBJECT = f"{TEARDOWN_PROCESSOR}.drop_generic_object"

RUN_PROCESSOR_GET_EXISTING_APP_INFO = f"{RUN_PROCESSOR}.get_existing_app_info"
RUN_PROCESSOR_GET_EXISTING_APP_PKG_INFO = f"{RUN_PROCESSOR}.get_existing_app_pkg_info"

FIND_VERSION_FROM_MANIFEST = f"{VERSION_MODULE}.find_version_info_in_manifest_file"

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


def mock_execute_helper(mock_input: list):
    side_effects, expected = map(list, zip(*mock_input))
    return side_effects, expected
