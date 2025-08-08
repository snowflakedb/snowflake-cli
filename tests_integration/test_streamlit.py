import pytest

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

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1_stage/app_1/app_1.py", "app_1_stage/app_1/streamlit_app.py"],
            f"{database}.public.app_1_stage",
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
            ["app_1_stage/app_1/app_1.py", "app_1_stage/app_1/streamlit_app.py"],
            f"{database}.public.app_1_stage",
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
            "app_1", snowflake_session, experimental=True
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
            "app_1", snowflake_session, experimental=True
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
def test_streamlit_spcs_runtime_v2_flow(
    _streamlit_test_steps,
    project_directory,
    test_database,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test SPCS runtime v2 functionality with both feature flags enabled."""
    import os
    from unittest import mock

    database = test_database.upper()
    entity_name = "streamlit_app"

    # Enable both required feature flags
    with (
        mock.patch.dict(
            os.environ,
            {
                "SNOWFLAKE_CLI_FEATURES_ENABLE_STREAMLIT_SPCS_RUNTIME_V2": "true",
                "SNOWFLAKE_CLI_FEATURES_ENABLE_STREAMLIT_VERSIONED_STAGE": "true",
            },
        )
    ):
        with project_directory("streamlit_spcs_v2"):
            _streamlit_test_steps.list_streamlit_should_return_empty_list()
            result = _streamlit_test_steps.setup.runner.invoke_with_connection_json(
                ["streamlit", "deploy", entity_name]
            )
            assert result.exit_code == 0, f"Deploy failed: {result.output}"

            # Verify entity was created
            deployed_name = "streamlit_spcs_v2_app"  # The actual name from identifier
            _streamlit_test_steps.assert_that_only_those_entities_are_listed(
                [f"{database}.PUBLIC.STREAMLIT_SPCS_V2_APP"], deployed_name.upper()
            )

            # Verify files were uploaded to versioned stage
            stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.streamlit_spcs_v2_app/versions/live/"
            _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
                [
                    "streamlit_app.py",
                    "utils/utils.py",
                    "pages/my_page.py",
                    "requirements.txt",
                ],
                stage_root,
                uploaded_to_live_version=True,
            )

            # Test describe and get-url (skip execute since SPCS resources don't exist in test env)
            deployed_name = "streamlit_spcs_v2_app"  # The actual name from identifier

            # Verify describe shows SPCS runtime v2 configuration
            result = _streamlit_test_steps.setup.runner.invoke_with_connection_json(
                ["streamlit", "describe", deployed_name]
            )
            assert result.exit_code == 0, f"Describe failed: {result.output}"
            assert len(result.json) == 1
            assert result.json[0]["name"] == deployed_name.upper()

            # Validate SPCS Runtime V2 fields are present in describe output
            streamlit_info = result.json[0]
            assert (
                "runtime_name" in streamlit_info
            ), "runtime_name field missing from describe output"
            assert (
                streamlit_info["runtime_name"] == "SYSTEM$ST_CONTAINER_RUNTIME_PY3_11"
            )
            assert (
                "compute_pool" in streamlit_info
            ), "compute_pool field missing from describe output"
            assert streamlit_info["compute_pool"] == "TEST_COMPUTE_POOL"
            _streamlit_test_steps.get_url_should_give_proper_url(
                deployed_name, snowflake_session
            )

            # Clean up
            _streamlit_test_steps.drop_should_succeed(deployed_name, snowflake_session)
            _streamlit_test_steps.list_streamlit_should_return_empty_list()
