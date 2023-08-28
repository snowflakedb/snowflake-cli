import pytest

from tests_integration.conftest import runner


def test_token(runner):
    result = runner.invoke_integration(["]snowpark", "registry", "token"])

    assert result.exit_code == 0