import pytest

from tests_integration.testing_utils import FlowTestSetup
from tests_integration.testing_utils.streamlit_utils import StreamlitTestSteps


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
    """Test SPCS runtime v2 functionality with experimental flag."""
    database = test_database.upper()
    entity_name = "streamlit_app"

    with project_directory("streamlit_spcs_v2"):
        _streamlit_test_steps.list_streamlit_should_return_empty_list()
        result = _streamlit_test_steps.setup.runner.invoke_with_connection_json(
            ["streamlit", "deploy", "--experimental", entity_name]
        )
        assert result.exit_code == 0, f"Deploy failed: {result.output}"

        # Verify entity was created
        deployed_name = "streamlit_spcs_v2_app"  # The actual name from identifier
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.STREAMLIT_SPCS_V2_APP"], deployed_name.upper()
        )

        # Verify core files were uploaded to versioned stage
        stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.streamlit_spcs_v2_app/versions/live/"
        actual_files = _streamlit_test_steps.get_actual_file_staged_in_db(stage_root)
        actual_files = {file.removeprefix("/versions/live/") for file in actual_files}

        # Check that the core files are present
        expected_core_files = {
            "streamlit_app.py",
            "utils/utils.py",
            "pages/my_page.py",
            "requirements.txt",
        }
        assert expected_core_files.issubset(
            actual_files
        ), f"Missing core files. Expected: {expected_core_files}, Got: {actual_files}"

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
        assert streamlit_info["runtime_name"] == "SYSTEM$ST_CONTAINER_RUNTIME_PY3_11"
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
