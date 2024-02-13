import uuid
from typing import Tuple

import pytest

from tests_integration.spcs.testing_utils.compute_pool_utils import (
    ComputePoolTestSetup,
    ComputePoolTestSteps,
)


@pytest.mark.integration
def test_compute_pool(_test_steps: Tuple[ComputePoolTestSteps, str]):

    test_steps, compute_pool_name = _test_steps

    test_steps.create_compute_pool(compute_pool_name)
    test_steps.list_should_return_compute_pool(compute_pool_name)
    test_steps.stop_all_on_compute_pool(compute_pool_name)
    test_steps.suspend_compute_pool(compute_pool_name)
    test_steps.wait_until_compute_pool_is_suspended(compute_pool_name)
    test_steps.resume_compute_pool(compute_pool_name)
    test_steps.wait_until_compute_pool_is_idle(compute_pool_name)
    test_steps.set_unset_compute_pool_property(compute_pool_name)
    test_steps.drop_compute_pool(compute_pool_name)
    test_steps.list_should_not_return_compute_pool(compute_pool_name)


@pytest.fixture
def _test_setup(runner, snowflake_session):
    compute_pool_test_setup = ComputePoolTestSetup(
        runner=runner, snowflake_session=snowflake_session
    )
    yield compute_pool_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    compute_pool_name = f"compute_pool_{uuid.uuid4().hex}"
    test_steps = ComputePoolTestSteps(_test_setup)

    yield test_steps, compute_pool_name

    _test_setup.snowflake_session.execute_string(
        f"drop compute pool if exists {compute_pool_name}"
    )
