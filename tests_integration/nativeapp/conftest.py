from contextlib import contextmanager

import pytest

from tests_integration.conftest import SnowCLIRunner


@pytest.fixture
def nativeapp_project_directory(project_directory, nativeapp_teardown):
    @contextmanager
    def _nativeapp_project_directory(name):
        with project_directory(name) as d:
            with nativeapp_teardown():
                yield d

    return _nativeapp_project_directory


@pytest.fixture
def nativeapp_teardown(runner: SnowCLIRunner):
    @contextmanager
    def _nativeapp_teardown():
        try:
            yield
        finally:
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force", "--cascade"]
            )
            assert result.exit_code == 0

    return _nativeapp_teardown
