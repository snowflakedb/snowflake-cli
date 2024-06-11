# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from tests_integration.spcs.testing_utils.spcs_jobs_utils import (
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
