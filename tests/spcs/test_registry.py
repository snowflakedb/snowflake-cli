import json

from tests.testing_utils.fixtures import *


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_registry_get_token_2(mock_execute, mock_conn, mock_cursor, runner):
    mock_execute.return_value = mock_cursor(
        ["row"], ["Statement executed successfully"]
    )
    mock_conn._rest._token_request.return_value = {
        "data": {
            "sessionToken": "token1234",
            "validityInSecondsST": 42,
        }
    }
    result = runner.invoke(["spcs", "image-registry", "token", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"token": "token1234", "expires_in": 42}
