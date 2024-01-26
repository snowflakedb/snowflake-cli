import uuid

import pytest

from tests_integration.testing_utils.snowpark_services_utils import (
    SnowparkServicesTestSetup,
    SnowparkServicesTestSteps,
)


@pytest.mark.integration
def test_services(_test_steps: SnowparkServicesTestSteps):
    service_name = f"snowpark_service_{uuid.uuid4().hex}"

    _test_steps.create_service(service_name)
    _test_steps.status_should_return_service(service_name)
    _test_steps.list_should_return_service(service_name)
    _test_steps.wait_until_service_will_be_finish(service_name)
    _test_steps.logs_should_return_service_logs(service_name)
    _test_steps.describe_should_return_service(service_name)
    _test_steps.drop_service(service_name)
    _test_steps.list_should_not_return_service(service_name)


@pytest.fixture
def _test_setup(
    runner,
    snowflake_session,
    test_root_path,
):
    snowpark_function_test_setup = SnowparkServicesTestSetup(
        runner=runner,
        snowflake_session=snowflake_session,
        test_root_path=test_root_path,
    )
    yield snowpark_function_test_setup


@pytest.fixture
def _test_steps(_test_setup):
    yield SnowparkServicesTestSteps(_test_setup)
