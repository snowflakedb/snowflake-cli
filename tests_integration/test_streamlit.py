from pathlib import Path

import pytest

from tests_common import IS_WINDOWS
from tests_integration.snowflake_connector import add_uuid_to_name
from tests_integration.testing_utils import FlowTestSetup
from tests_integration.testing_utils.streamlit_utils import StreamlitTestSteps

# TODO: use below constant instead of hardcoded values
APP_1 = "app_1"


@pytest.mark.integration
def test_streamlit_flow(
    _streamlit_test_steps,
    project_directory,
    test_database,
    snowflake_session,
    alter_snowflake_yml,
):
    database = test_database.upper()
    with project_directory("streamlit_v2"):
        _streamlit_test_steps.list_streamlit_should_return_empty_list()

        _streamlit_test_steps.deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file()

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            "app_1", snowflake_session
        )

        stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.app_1/versions/live/"

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.another_deploy_without_replace_flag_should_end_with_error(
            "app_1", snowflake_session
        )
        _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
            "app_1", snowflake_session
        )

        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )

        _streamlit_test_steps.streamlit_describe_should_show_proper_streamlit(
            APP_1, snowflake_session
        )

        _streamlit_test_steps.get_url_should_give_proper_url(APP_1, snowflake_session)

        _streamlit_test_steps.execute_should_run_streamlit(APP_1, snowflake_session)

        _streamlit_test_steps.drop_should_succeed(APP_1, snowflake_session)

        _streamlit_test_steps.list_streamlit_should_return_empty_list()


@pytest.mark.integration
def test_streamlit_experimental_flow(
    _streamlit_test_steps,
    project_directory,
    test_database,
    snowflake_session,
    alter_snowflake_yml,
):
    database = test_database.upper()
    with project_directory("streamlit_v2"):
        _streamlit_test_steps.list_streamlit_should_return_empty_list()

        _streamlit_test_steps.deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file()

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            "app_1", snowflake_session
        )

        stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.app_1/versions/live/"

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.another_deploy_without_replace_flag_should_end_with_error(
            "app_1", snowflake_session
        )
        _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
            "app_1", snowflake_session
        )

        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )

        _streamlit_test_steps.streamlit_describe_should_show_proper_streamlit(
            APP_1, snowflake_session
        )

        _streamlit_test_steps.get_url_should_give_proper_url(APP_1, snowflake_session)

        _streamlit_test_steps.execute_should_run_streamlit(APP_1, snowflake_session)

        _streamlit_test_steps.drop_should_succeed(APP_1, snowflake_session)

        _streamlit_test_steps.list_streamlit_should_return_empty_list()


@pytest.fixture
def _test_setup(
    runner, sql_test_helper, test_database, temporary_working_directory, snapshot
):
    yield FlowTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot,
    )


@pytest.fixture
def _streamlit_test_steps(_test_setup):
    return StreamlitTestSteps(_test_setup)


@pytest.mark.integration
def test_streamlit_grants_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test that streamlit grants are properly applied during deployment."""
    test_role = snowflake_session.role
    entity_id = "app_1"

    with project_directory("streamlit_v2"):
        alter_snowflake_yml(
            "snowflake.yml",
            "entities.app_1.grants",
            [{"privilege": "USAGE", "role": test_role}],
        )

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            entity_id, snowflake_session, experimental=False
        )

        _streamlit_test_steps.verify_grants_applied(entity_id, test_role)

        _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)


@pytest.mark.integration
def test_streamlit_grants_experimental_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test that streamlit grants are properly applied during experimental deployment."""
    test_role = snowflake_session.role
    entity_id = "app_1"

    with project_directory("streamlit_v2"):
        alter_snowflake_yml(
            "snowflake.yml",
            "entities.app_1.grants",
            [{"privilege": "USAGE", "role": test_role}],
        )

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            entity_id, snowflake_session
        )

        _streamlit_test_steps.verify_grants_applied(entity_id, test_role)

        _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)


@pytest.mark.integration
@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
class TestFollowSymlinksFlow:
    def test_escaping_symlink_blocked_without_flag(
        self,
        runner,
        project_directory,
        alter_snowflake_yml,
        test_database,
        snowflake_session,
        tmp_path,
    ):
        outside_file = tmp_path / "external_module.py"
        outside_file.write_text("# content outside project")

        with project_directory("streamlit_v2"):
            Path("external_module.py").symlink_to(outside_file)
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.artifacts",
                ["streamlit_app.py", "app_1.py", "external_module.py"],
            )

            result = runner.invoke_with_connection(["streamlit", "deploy", "app_1"])

            assert result.exit_code != 0
            assert "--follow-symlinks" in result.output

    def test_escaping_symlink_allowed_with_flag(
        self,
        runner,
        project_directory,
        alter_snowflake_yml,
        test_database,
        snowflake_session,
        tmp_path,
    ):
        outside_file = tmp_path / "external_module.py"
        outside_file.write_text("# content outside project")
        app_name = add_uuid_to_name("app")

        with project_directory("streamlit_v2"):
            Path("external_module.py").symlink_to(outside_file)
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.artifacts",
                ["streamlit_app.py", "app_1.py", "external_module.py"],
            )
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.identifier.name",
                app_name,
            )

            try:
                result = runner.invoke_with_connection(
                    ["streamlit", "deploy", "app_1", "--follow-symlinks"]
                )

                assert result.exit_code == 0
                assert "--follow-symlinks is set" in result.output
            finally:
                runner.invoke_with_connection(["streamlit", "drop", "app_1"])
