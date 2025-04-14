import pytest

from test_data.projects.stage_execute.script_template import database
from testing_utils import FlowTestSetup
from testing_utils.streamlit_utils import StreamlitTestSteps


@pytest.mark.integration
def test_streamlit_flow(
        _streamlit_test_steps,
        project_directory,
        test_database
):
    #list streamlit - should be empty
    database = test_database.upper()
    with project_directory():
        _streamlit_test_steps.list_streamlit_should_return_empty_list(database)
    #prepare test snowflake.yml with multiple streamlits

    #how to check if browser wasn't called?
    # run bare deploy - should end up in error - or maybe it can be left in units?

    # deploy normally
    # * should deploy streamlit and return proper code
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

    #deploy experimental

    pass
@pytest.fixture
def _test_setup(
        runner,
        sql_test_helper,
        test_database,
        temporary_working_directory,
        snapshot):
    yield FlowTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot
    )

@pytest.fixture
def _streamlit_test_steps(_test_setup):
    return StreamlitTestSteps(_test_setup)