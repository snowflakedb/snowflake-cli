import datetime
import uuid

import pytest

_SNOW_CLI_INTEGRATION_TESTS_NAME_PREFIX = "snowcli_integration_tests"


class ObjectNameProvider:
    def __init__(self, test_name: str):
        self._object_name_prefix = self._create_object_name_prefix(test_name)
        self._counter = 0

    def get_object_name_prefix(self) -> str:
        return self._object_name_prefix

    def create_and_get_next_object_name(self) -> str:
        self._counter += 1
        return self._object_name_prefix + "__" + self._counter.__str__()

    def is_name_from_this_test(self, name: str) -> bool:
        return name.lower().startswith(self._object_name_prefix.lower())

    @staticmethod
    def _create_object_name_prefix(test_name: str) -> str:
        dt_now_str = datetime.datetime.now().strftime("%Y_%m_%d__%H_%M_%S_%f")
        test_id = uuid.uuid4().hex
        return f"{_SNOW_CLI_INTEGRATION_TESTS_NAME_PREFIX}__{dt_now_str}__{test_id}__{test_name}"


@pytest.fixture
def object_name_provider(request):
    name_provider = ObjectNameProvider(request.node.originalname)
    yield name_provider
