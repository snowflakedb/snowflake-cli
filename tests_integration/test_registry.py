import pytest


@pytest.mark.integration
def test_token(runner):
    result = runner.invoke_integration(["snowpark", "registry", "token","-c", "integration"])

    assert result.exit_code == 0
