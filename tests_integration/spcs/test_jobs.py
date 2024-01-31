import pytest

from tests_integration.testing_utils.snowpark_jobs_utils import (
    SnowparkJobsTestSetup,
    SnowparkJobsTestSteps,
)


@pytest.mark.skip("Snowpark Container Services Job not supported.")
@pytest.mark.integration
def test_jobs(_test_steps: SnowparkJobsTestSteps):

    job_id = _test_steps.create_job()
    _test_steps.status_should_return_job(job_id)
    _test_steps.logs_should_return_job_logs(job_id)
    _test_steps.drop_job(job_id)


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    test_root_path,
):
    snowpark_function_test_setup = SnowparkJobsTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        test_root_path=test_root_path,
    )
    yield snowpark_function_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkJobsTestSteps(_test_setup)
