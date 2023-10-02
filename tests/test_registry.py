import json
from unittest import mock
from tests.testing_utils.fixtures import *


@mock.patch(
    "snowcli.cli.registry.manager.snow_cli_global_context_manager.get_connection"
)
def test_registry_get_token(mock_conn, runner):
    mock_conn.return_value._rest._token_request.return_value = {
        "data": {
            "sessionToken": "token1234",
            "validityInSecondsST": 42,
        }
    }
    result = runner.invoke(["registry", "token", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"token": "token1234", "expires_in": 42}
