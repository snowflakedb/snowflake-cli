import pytest

from tests_integration.testing_utils import FlowTestSetup
from tests_integration.testing_utils.streamlit_utils import StreamlitTestSteps

# TODO: use below constant instead of hardcoded values
APP_1 = "app_1"


@pytest.mark.integration
def test_streamlit_flow(
    _streamlit_test_steps, project_directory, test_database, alter_snowflake_yml
):

    database = test_database.upper()
    with project_directory("streamlit_v2"):
        _streamlit_test_steps.list_streamlit_should_return_empty_list()

        _streamlit_test_steps.deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file()

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            "app_1", database
        )
        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1_stage/app_1/app_1.py", "app_1_stage/app_1/streamlit_app.py"],
            f"{database}.public.app_1_stage",
        )
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.another_deploy_without_replace_flag_should_end_with_error(
            "app_1", database
        )
        _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
            "app_1", database
        )

        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1_stage/app_1/app_1.py", "app_1_stage/app_1/streamlit_app.py"],
            f"{database}.public.app_1_stage",
        )

        _streamlit_test_steps.streamlit_describe_should_show_proper_streamlit(
            APP_1, database
        )

        _streamlit_test_steps.get_url_should_give_proper_url(APP_1, database)

        _streamlit_test_steps.execute_should_run_streamlit(APP_1, database)

        _streamlit_test_steps.drop_should_succeed(APP_1, database)

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
