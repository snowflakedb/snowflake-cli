import pytest

from tests_integration.testing_utils import FlowTestSetup
from tests_integration.testing_utils.streamlit_utils import StreamlitTestSteps


@pytest.mark.integration
def test_streamlit_flow(_streamlit_test_steps, project_directory, test_database):
    # list streamlit - should be empty
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

    # how to check if browser wasn't called? - this shoulg go to the units, with mocked entity

    # * should return proper url
    # * check for files in stage should have only expected files

    # List should show deployed streamlit
    # describe should show streamlit
    # execute streamlit should execute streamlit

    # get url should give proper url

    # another deploy should end with error

    # if not exists should return proper message
    # replace should deploy without any comments

    # drop should drop streamlit

    # list should be empty

    # deploy experimental

    pass


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
