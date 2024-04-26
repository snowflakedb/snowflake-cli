from unittest import mock

import pytest


@pytest.fixture(name="_patch_app_version_in_tests", autouse=True)
def mock_app_version_in_tests(request):
    """Set predefined Snowflake-CLI for testing.

    Marker `app_version_patch` can be used in tests to skip it.

    @pytest.mark.app_version_patch(False)
    def test_case():
        ...
    """
    marker = request.node.get_closest_marker("app_version_patch")

    if marker and marker.kwargs.get("use") is False:
        yield
    else:
        with mock.patch(
            "snowflake.cli.__about__.VERSION", "0.0.0-test_patched"
        ) as _fixture:
            yield _fixture
