import pytest


@pytest.mark.integration
def test_token(runner):
    result = runner.invoke_with_connection_json(["spcs", "image-registry", "token"])

    assert result.exit_code == 0
    assert result.json
    assert "token" in result.json
    assert result.json["token"]
    assert "expires_in" in result.json
    assert result.json["expires_in"]
