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

import webbrowser
from textwrap import dedent

import pytest
import tomlkit
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.connector.errors import Error as SnowflakeError

from tests_common.feature_flag_utils import with_feature_flags


def _enable_auth():
    return with_feature_flags({FeatureFlag.ENABLE_AUTH_COMMANDS: True})


def _panel_text(output: str) -> str:
    """Error panel text with the box border removed, so wrapping does not split a sentence."""
    lines = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("+"):
            continue
        if stripped.startswith("|"):
            stripped = stripped[1:]
        if stripped.endswith("|"):
            stripped = stripped[:-1]
        lines.append(stripped.strip())
    return " ".join(lines)


class _Cursor:
    def __init__(self, user: str):
        self._user = user

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, _query, *_args, **_kwargs):
        return self

    def fetchone(self):
        return (self._user,)


class _Connection:
    def __init__(self, user: str):
        self._user = user

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return _Cursor(self._user)

    def close(self):
        return None


def _patch_sign_in(monkeypatch, users: list[str]):
    """Supply the session user each connect() would read. No browser, no keychain."""
    calls: list[dict] = []
    remaining = list(users)

    def connect(**kwargs):
        calls.append(kwargs)
        if not remaining:
            raise AssertionError("login connected again after the session was stored")
        return _Connection(remaining.pop(0))

    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.snowflake.connector.connect",
        connect,
    )
    return calls


_OTHER_CONNECTION = dedent(
    """\
    [connections.keypair]
    account = "other-acct"
    user = "JDOE"
    database = "ANALYTICS"
    schema = "PUBLIC"
    authenticator = "SNOWFLAKE_JWT"
    """
)


def test_login_writes_a_local_oauth_connection_and_sets_the_default(
    runner, snowflake_home, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )

        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "changed the default connection to myorg-myacct" in result.output
    assert str(path) in result.output
    assert calls == [
        {
            "account": "myorg-myacct",
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "application": "SNOWCLI.AUTH.LOGIN",
            "client_store_temporary_credential": False,
        },
        {
            "account": "myorg-myacct",
            "user": "JDOE",
            "authenticator": "OAUTH_AUTHORIZATION_CODE",
            "application": "SNOWCLI.AUTH.LOGIN",
            "client_store_temporary_credential": True,
        },
    ]
    connection = written["connections"]["myorg-myacct"]
    assert dict(connection) == {
        "account": "myorg-myacct",
        "user": "JDOE",
        "authenticator": "OAUTH_AUTHORIZATION_CODE",
        "client_store_temporary_credential": True,
    }
    assert written["default_connection_name"] == "myorg-myacct"
    assert written["connections"]["keypair"]["database"] == "ANALYTICS"
    assert written["connections"]["keypair"]["schema"] == "PUBLIC"
    assert not (snowflake_home / "connections.toml").exists()


def test_login_is_hidden_when_the_feature_flag_is_off(runner):
    result = runner.invoke(["auth", "--help"])

    assert result.exit_code == 0, result.output
    assert "login" not in result.output
    assert "oidc" in result.output


def test_login_is_shown_when_the_feature_flag_is_on(runner):
    with _enable_auth():
        result = runner.invoke(["auth", "--help"])

    assert result.exit_code == 0, result.output
    assert "login" in result.output
    assert "list" in result.output
    assert "oidc" in result.output


def test_login_help_says_the_default_connection_changes(runner):
    with _enable_auth():
        result = runner.invoke(["auth", "login", "--help"])

    assert result.exit_code == 0, result.output
    assert "changes the default connection" in result.output
    assert "public account host" in result.output
    assert "--host" in result.output


def test_login_prompts_for_the_account_and_nothing_else(
    runner, snowflake_home, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login"], input="myorg-myacct\n"
            )

    assert result.exit_code == 0, result.output
    assert "Enter account" in result.output
    assert "Enter user" not in result.output
    assert "Enter password" not in result.output
    assert "Enter role" not in result.output
    assert "changed the default connection to myorg-myacct" in result.output


def test_login_without_an_account_writes_nothing(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(str(path), ["auth", "login", "   "])
        written = tomlkit.parse(path.read_text())

    assert result.exit_code != 0, result.output
    assert "Enter an account" in result.output
    assert calls == []
    assert list(written["connections"].keys()) == ["keypair"]
    assert "default_connection_name" not in written


@pytest.mark.parametrize(
    ("argument", "host_flag", "account", "host"),
    [
        (
            "myorg-myacct",
            "myorg-myacct.privatelink.snowflakecomputing.com",
            "myorg-myacct",
            "myorg-myacct.privatelink.snowflakecomputing.com",
        ),
        (
            "https://myorg-myacct.qa6.snowflakecomputing.com/console",
            None,
            "myorg-myacct",
            "myorg-myacct.qa6.snowflakecomputing.com",
        ),
        (
            "https://myorg-myacct.snowflakecomputing.mil",
            None,
            "myorg-myacct",
            "myorg-myacct.snowflakecomputing.mil",
        ),
        (
            "xy12345-extid.global.snowflakecomputing.com",
            None,
            "xy12345",
            "xy12345-extid.global.snowflakecomputing.com",
        ),
        (
            "https://myorg-myacct.privatelink.snowflakecomputing.com",
            "myorg-myacct.snowflakecomputing.mil",
            "myorg-myacct",
            "myorg-myacct.snowflakecomputing.mil",
        ),
    ],
)
def test_login_stores_a_host_only_when_given_one(
    runner,
    config_file,
    monkeypatch,
    argument,
    host_flag,
    account,
    host,
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    args = ["auth", "login", argument]
    if host_flag:
        args.extend(["--host", host_flag])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(str(path), args)
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert calls[0]["account"] == account
    assert calls[0]["host"] == host
    assert "user" not in calls[0]
    assert calls[0]["client_store_temporary_credential"] is False
    assert calls[1]["account"] == account
    assert calls[1]["host"] == host
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["client_store_temporary_credential"] is True
    connection = written["connections"][account]
    assert connection["account"] == account
    assert connection["host"] == host
    assert connection["user"] == "JDOE"
    assert "database" not in connection
    assert "schema" not in connection
    assert written["default_connection_name"] == account


def test_login_writes_to_the_connections_file_when_it_already_exists(
    runner, snowflake_home, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    connections_file = snowflake_home / "connections.toml"
    connections_file.write_text(
        dedent(
            """\
            [keypair]
            account = "other-acct"
            user = "JDOE"
            database = "ANALYTICS"
            schema = "PUBLIC"
            authenticator = "SNOWFLAKE_JWT"
            """
        )
    )
    connections_file.chmod(0o600)
    with config_file("") as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        config = tomlkit.parse(path.read_text())
    connections = tomlkit.parse(connections_file.read_text())

    assert result.exit_code == 0, result.output
    assert str(connections_file) in result.output
    assert "changed the default connection to myorg-myacct" in result.output
    assert calls[0]["account"] == "myorg-myacct"
    assert "host" not in calls[0]
    assert "connections" not in config
    assert config["default_connection_name"] == "myorg-myacct"
    assert connections["keypair"]["database"] == "ANALYTICS"
    assert connections["keypair"]["schema"] == "PUBLIC"
    assert "database" not in connections["myorg-myacct"]
    assert "schema" not in connections["myorg-myacct"]
    assert connections["myorg-myacct"]["user"] == "JDOE"
    assert connections["myorg-myacct"]["authenticator"] == "OAUTH_AUTHORIZATION_CODE"


def test_login_finishes_through_the_connector_without_opening_a_browser(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])

    def explode(*_args, **_kwargs):
        raise AssertionError("login opened a browser")

    monkeypatch.setattr(webbrowser, "open", explode)
    monkeypatch.setattr(webbrowser, "open_new", explode)
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )

    assert result.exit_code == 0, result.output
    assert "changed the default connection to myorg-myacct" in result.output


def test_a_rejected_sign_in_writes_nothing(runner, config_file, monkeypatch):
    def connect(**_kwargs):
        raise SnowflakeError("The account refused the sign-in.")

    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.snowflake.connector.connect",
        connect,
    )
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    assert "refused the sign-in" in result.output
    assert "Check the account and try again." in _panel_text(result.output)
    assert after == before


def test_a_second_sign_in_as_someone_else_writes_nothing(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "SSMITH"])
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    assert "SSMITH" in result.output
    assert "JDOE" in result.output
    assert after == before


def test_login_does_not_clear_database_or_schema_on_an_existing_connection(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    config = dedent(
        """\
        [connections.myorg-myacct]
        account = "myorg-myacct"
        user = "OLD"
        database = "ANALYTICS"
        schema = "PUBLIC"
        warehouse = "COMPUTE_WH"
        authenticator = "SNOWFLAKE_JWT"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    connection = written["connections"]["myorg-myacct"]
    assert connection["database"] == "ANALYTICS"
    assert connection["schema"] == "PUBLIC"
    assert connection["warehouse"] == "COMPUTE_WH"
    assert connection["user"] == "JDOE"
    assert connection["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["default_connection_name"] == "myorg-myacct"


def test_a_bare_account_login_uses_the_host_already_stored(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    stored_host = "myorg-myacct.privatelink.snowflakecomputing.com"
    config = dedent(
        f"""\
        [connections.myorg-myacct]
        account = "myorg-myacct"
        user = "OLD"
        host = "{stored_host}"
        database = "ANALYTICS"
        authenticator = "SNOWFLAKE_JWT"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert calls[0]["host"] == stored_host
    assert calls[0]["client_store_temporary_credential"] is False
    assert calls[1]["host"] == stored_host
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["client_store_temporary_credential"] is True
    connection = written["connections"]["myorg-myacct"]
    assert connection["host"] == stored_host
    assert connection["user"] == "JDOE"
    assert connection["database"] == "ANALYTICS"
    assert connection["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["default_connection_name"] == "myorg-myacct"


def test_a_host_flag_replaces_the_host_already_stored(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    replacement = "myorg-myacct.snowflakecomputing.mil"
    config = dedent(
        """\
        [connections.myorg-myacct]
        account = "myorg-myacct"
        user = "OLD"
        host = "myorg-myacct.privatelink.snowflakecomputing.com"
        authenticator = "SNOWFLAKE_JWT"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "--host", replacement, "myorg-myacct"],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert calls[0]["host"] == replacement
    assert calls[1]["host"] == replacement
    assert written["connections"]["myorg-myacct"]["host"] == replacement
    assert written["connections"]["myorg-myacct"]["user"] == "JDOE"


@pytest.mark.parametrize("connector_v5", [False, True])
def test_login_stores_the_session_user_when_only_the_case_differs(
    runner, config_file, monkeypatch, connector_v5
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.IS_V5_DRIVER",
        connector_v5,
    )
    calls = _patch_sign_in(monkeypatch, ["JDOE", "jdoe"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert calls[1]["user"] == "JDOE"
    assert written["connections"]["myorg-myacct"]["user"] == "JDOE"


def test_login_prints_an_account_name_that_looks_like_rich_markup(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    account = "org[/x]acct"
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", account]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert f"changed the default connection to {account}" in result.output
    assert written["connections"][account]["user"] == "JDOE"
    assert written["default_connection_name"] == account


def test_a_quoted_session_user_that_differs_by_case_is_rejected_on_connector_5(
    runner, config_file, monkeypatch
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.IS_V5_DRIVER",
        True,
    )
    _patch_sign_in(monkeypatch, ['"JDoe"', '"jdoe"'])
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    flat = _panel_text(result.output)
    assert '"JDoe"' in flat
    assert '"jdoe"' in flat
    assert "Sign in as the same user and try again." in flat
    assert after == before


def test_connector_4_accepts_a_quoted_session_user_that_differs_only_by_case(
    runner, config_file, monkeypatch
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.IS_V5_DRIVER",
        False,
    )
    calls = _patch_sign_in(monkeypatch, ['"JDoe"', '"jdoe"'])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert calls[1]["user"] == '"JDoe"'
    assert written["connections"]["myorg-myacct"]["user"] == '"JDoe"'
