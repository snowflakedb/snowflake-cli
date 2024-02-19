import uuid
from typing import Tuple

import pytest

from tests_integration.spcs.testing_utils.spcs_services_utils import (
    SnowparkServicesTestSetup,
    SnowparkServicesTestSteps,
)


@pytest.mark.integration
def test_services(_test_steps: Tuple[SnowparkServicesTestSteps, str]):

    test_steps, service_name = _test_steps

    test_steps.create_service(service_name)
    test_steps.status_should_return_service(service_name)
    test_steps.list_should_return_service(service_name)
    test_steps.wait_until_service_is_ready(service_name)
    test_steps.logs_should_return_service_logs(service_name)
    test_steps.suspend_service(service_name)
    test_steps.wait_until_service_is_suspended(service_name)
    test_steps.resume_service(service_name)
    test_steps.wait_until_service_is_ready(service_name)
    test_steps.describe_should_return_service(service_name)
    test_steps.list_endpoints_should_show_endpoint(service_name)
    test_steps.upgrade_service_should_change_spec(service_name)
    test_steps.set_unset_service_property(service_name)
    test_steps.drop_service(service_name)
    test_steps.list_should_not_return_service(service_name)


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
    service_name = f"spcs_service_{uuid.uuid4().hex}"
    test_steps = SnowparkServicesTestSteps(_test_setup)

    yield test_steps, service_name

    service_fqn = f"{test_steps.database}.{test_steps.schema}.{service_name}"
    _test_setup.snowflake_session.execute_string(
        f"drop service if exists {service_fqn}"
    )
