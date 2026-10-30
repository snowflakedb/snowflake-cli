from unittest import mock

import pytest
from snowflake.cli._app import main_typer, snow_connector
from snowflake.cli._plugins.ai import commands
from snowflake.cli.api.connections import ConnectionContext


@pytest.mark.parametrize("temporary", [False, True])
def test_launcher_and_connector_share_typed_resolution(temporary):
    context = ConnectionContext(
        connection_name=None if temporary else "test",
        temporary_connection=temporary,
        role="flag_role",
    )
    config = {
        "user": "configured_user",
        "oauth_enable_refresh_tokens": "true",
        "client_store_temporary_credential": "false",
        "proxy_host": "configured-proxy",
    }
    environment = {
        "user": "env_user",
        "oauth_enable_refresh_tokens": "false",
        "client_store_temporary_credential": "true",
        "role": "env_role",
    }
    with mock.patch.object(
        snow_connector, "get_connection_dict", return_value=config
    ), mock.patch.object(
        snow_connector, "get_env_value", side_effect=lambda key: environment.get(key)
    ), mock.patch.object(
        commands, "get_cli_context"
    ) as cli_context:
        cli_context.return_value.connection_context = context
        effective = getattr(commands, "_effective_connection_context")()
        parameters = snow_connector.resolve_connection_parameters(
            **context.present_values_as_dict()
        )
    for name in (
        "user",
        "role",
        "oauth_enable_refresh_tokens",
        "client_store_temporary_credential",
    ):
        assert getattr(effective, name) == parameters[name]
    assert effective.role == "flag_role"
    assert effective.oauth_enable_refresh_tokens is (not temporary)
    assert effective.client_store_temporary_credential is temporary
    if not temporary:
        assert effective.proxy_host == "configured-proxy"
    assert context.user is None


@pytest.mark.parametrize("style", ["separate", "equals"])
def test_config_prescan_ignores_agent_tail(tmp_path, style):
    config = tmp_path / "config.toml"
    config.write_text("")
    option = (
        ["--config-file", str(config)]
        if style == "separate"
        else [f"--config-file={config}"]
    )
    pre_scan = getattr(main_typer, "_maybe_init_config_from_args")
    with mock.patch.object(main_typer, "config_init") as initialize:
        pre_scan(["ai", "claude", "--agent-args", *option])
        initialize.assert_not_called()
        pre_scan([*option, "ai", "claude", "--agent-args", "--config-file=ignored"])
        initialize.assert_called_once_with(config)
