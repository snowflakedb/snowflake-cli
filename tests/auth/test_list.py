# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import json
from textwrap import dedent

import typer
from snowflake.cli._plugins.auth import _AuthTyperFactory
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags


def _enable_auth():
    return with_feature_flags({FeatureFlag.ENABLE_AUTH_COMMANDS: True})


_MIXED_CONNECTIONS = dedent(
    """\
    default_connection_name = "acct-jdoe"

    [connections.acct-jdoe]
    account = "myorg-myacct"
    user = "JDOE"
    database = "ANALYTICS"
    schema = "PUBLIC"
    warehouse = "COMPUTE_WH"
    role = "SYSADMIN"
    authenticator = "OAUTH_AUTHORIZATION_CODE"
    client_store_temporary_credential = true

    [connections.acct-other]
    account = "other-acct"
    user = "SSMITH"
    authenticator = "oauth_authorization_code"

    [connections.acct-jdoe-privatelink]
    account = "myorg-myacct"
    user = "JDOE"
    host = "myorg-myacct.privatelink.snowflakecomputing.com"
    authenticator = "OAUTH_AUTHORIZATION_CODE"

    [connections.custom-oauth]
    account = "myorg-myacct"
    user = "JDOE"
    authenticator = "OAUTH_AUTHORIZATION_CODE"
    oauth_client_id = "client_id"
    oauth_client_secret = "client_secret"

    [connections.keypair]
    account = "myorg-myacct"
    user = "JDOE"
    authenticator = "SNOWFLAKE_JWT"
    private_key_file = "/private/key"

    [connections.password]
    account = "myorg-myacct"
    user = "JDOE"
    password = "secret"
    """
)


def test_list_is_hidden_when_the_feature_flag_is_off(runner):
    result = runner.invoke(["auth", "--help"])

    assert result.exit_code == 0, result.output
    assert "list" not in result.output
    assert "oidc" in result.output


def test_list_help_uses_oauth_for_local_applications(runner):
    with _enable_auth():
        result = runner.invoke(["auth", "list", "--help"])

    assert result.exit_code == 0, result.output
    assert "Snowflake OAuth for local applications" in result.output
    assert "oauth_client_id" in result.output
    assert "oauth_client_secret" in result.output
    assert "snow connection list" in result.output
    assert "local OAuth" not in result.output
    assert "Key pair" not in result.output


def _factory_with_status_and_oidc() -> _AuthTyperFactory:
    factory = _AuthTyperFactory(name="auth", help="Manages authentication methods.")
    oidc = SnowTyperFactory(name="oidc", help="Manages OIDC authentication.")

    @factory.command("status", requires_connection=False, requires_global_options=False)
    def status():
        """Shows status."""

    @oidc.command(
        "read-token", requires_connection=False, requires_global_options=False
    )
    def read_token():
        """Reads a token."""

    factory.add_typer(oidc)
    return factory


def _hidden(factory: _AuthTyperFactory, name: str) -> bool:
    group = typer.main.get_command(factory.create_instance())
    return group.commands[name].hidden


def test_later_auth_commands_are_hidden_with_list_when_the_flag_is_off():
    factory = _factory_with_status_and_oidc()

    assert _hidden(factory, "status") is True
    assert _hidden(factory, "oidc") is False


def test_later_auth_commands_are_visible_when_the_flag_is_on():
    factory = _factory_with_status_and_oidc()

    with _enable_auth():
        assert _hidden(factory, "status") is False
    assert _hidden(factory, "oidc") is False


def test_list_with_no_local_oauth_connections(runner, snowflake_home, config_file):
    config = dedent(
        """\
        default_connection_name = "keypair"

        [connections.keypair]
        account = "myorg-myacct"
        user = "JDOE"
        authenticator = "SNOWFLAKE_JWT"
        private_key_file = "/private/key"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(str(path), ["auth", "list"])

    assert result.exit_code == 0, result.output
    assert "No data" in result.output
    assert "keypair" not in result.output
    assert "/private/key" not in result.output


def test_connection_list_still_shows_every_connection(
    runner, snowflake_home, config_file
):
    with config_file(_MIXED_CONNECTIONS) as path:
        result = runner.invoke_with_config_file(
            str(path), ["connection", "list", "--format", "json"]
        )

    assert result.exit_code == 0, result.output
    names = {row["connection_name"] for row in json.loads(result.output)}
    assert names == {
        "acct-jdoe",
        "acct-other",
        "acct-jdoe-privatelink",
        "custom-oauth",
        "keypair",
        "password",
    }


def test_list_help_is_shown_when_the_feature_flag_is_on(runner):
    with _enable_auth():
        result = runner.invoke(["auth", "--help"])

    assert result.exit_code == 0, result.output
    assert "list" in result.output
    assert "oidc" in result.output


def test_list_shows_saved_local_oauth_connections(
    runner, snowflake_home, config_file, monkeypatch
):
    def connect(**kwargs):
        raise AssertionError("list called Snowflake")

    monkeypatch.setattr("snowflake.connector.connect", connect)
    with config_file(_MIXED_CONNECTIONS) as path:
        with _enable_auth():
            shown = runner.invoke_with_config_file(str(path), ["auth", "list"])
            listed = runner.invoke_with_config_file(
                str(path), ["auth", "list", "--format", "json"]
            )

    assert shown.exit_code == 0, shown.output
    assert "Entries from config.toml are ignored." not in shown.output
    assert "acct-jdoe" in shown.output
    assert "acct-other" in shown.output
    assert "acct-jdoe-privatelink" in shown.output
    assert "SSMITH" in shown.output
    assert "ANALYTICS" in shown.output
    assert "COMPUTE_WH" in shown.output
    assert "SYSADMIN" in shown.output
    assert "custom-oauth" not in shown.output
    assert "client_id" not in shown.output
    assert "client_secret" not in shown.output
    assert "keypair" not in shown.output
    assert "secret" not in shown.output
    assert "/private/key" not in shown.output
    assert "None" not in shown.output

    assert listed.exit_code == 0, listed.output
    payload = json.loads(listed.output)
    by_name = {row["connection_name"]: row for row in payload}
    assert by_name == {
        "acct-jdoe": {
            "connection_name": "acct-jdoe",
            "account": "myorg-myacct",
            "user": "JDOE",
            "host": "",
            "database": "ANALYTICS",
            "schema": "PUBLIC",
            "warehouse": "COMPUTE_WH",
            "role": "SYSADMIN",
            "is_default": True,
        },
        "acct-other": {
            "connection_name": "acct-other",
            "account": "other-acct",
            "user": "SSMITH",
            "host": "",
            "database": "",
            "schema": "",
            "warehouse": "",
            "role": "",
            "is_default": False,
        },
        "acct-jdoe-privatelink": {
            "connection_name": "acct-jdoe-privatelink",
            "account": "myorg-myacct",
            "user": "JDOE",
            "host": "myorg-myacct.privatelink.snowflakecomputing.com",
            "database": "",
            "schema": "",
            "warehouse": "",
            "role": "",
            "is_default": False,
        },
    }


def test_list_warns_when_connections_toml_replaces_config(
    runner, snowflake_home, config_file
):
    connections = snowflake_home / "connections.toml"
    connections.write_text(
        dedent(
            """\
            [from-connections]
            account = "from-connections-toml"
            user = "FROMCONN"
            authenticator = "OAUTH_AUTHORIZATION_CODE"
            """
        )
    )
    connections.chmod(0o600)
    config = dedent(
        """\
        [connections.only-in-config]
        account = "from-config"
        user = "FROMCONFIG"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(str(path), ["auth", "list"])

    assert result.exit_code == 0, result.output
    assert (
        f"Reading connections from {connections}. Entries from config.toml are ignored."
        in result.output
    )
    assert "from-connections" in result.output
    assert "FROMCONN" in result.output
    assert "only-in-config" not in result.output
    assert "FROMCONFIG" not in result.output
