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
    assert "changed the default connection to myorg-myacct-JDOE" in result.output
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
    connection = written["connections"]["myorg-myacct-JDOE"]
    assert dict(connection) == {
        "account": "myorg-myacct",
        "user": "JDOE",
        "authenticator": "OAUTH_AUTHORIZATION_CODE",
        "client_store_temporary_credential": True,
    }
    assert written["default_connection_name"] == "myorg-myacct-JDOE"
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
    assert "signs in each time" in result.output
    assert "--host" in result.output
    assert "--connection-name" in result.output


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
    assert "changed the default connection to myorg-myacct-JDOE" in result.output


def test_login_without_an_account_writes_nothing(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(str(path), ["auth", "login", "   "])
        written = tomlkit.parse(path.read_text())

    assert result.exit_code != 0, result.output
    assert "Enter an account" in result.output
    assert calls == []
    assert set(written["connections"]) == {"keypair"}
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
    name = f"{account}-JDOE"
    connection = written["connections"][name]
    assert connection["account"] == account
    assert connection["host"] == host
    assert connection["user"] == "JDOE"
    assert "database" not in connection
    assert "schema" not in connection
    assert written["default_connection_name"] == name


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
    assert "changed the default connection to myorg-myacct-JDOE" in result.output
    assert calls[0]["account"] == "myorg-myacct"
    assert "host" not in calls[0]
    assert "connections" not in config
    assert config["default_connection_name"] == "myorg-myacct-JDOE"
    assert connections["keypair"]["database"] == "ANALYTICS"
    assert connections["keypair"]["schema"] == "PUBLIC"
    assert "database" not in connections["myorg-myacct-JDOE"]
    assert "schema" not in connections["myorg-myacct-JDOE"]
    assert connections["myorg-myacct-JDOE"]["user"] == "JDOE"
    assert (
        connections["myorg-myacct-JDOE"]["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    )


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
    assert "changed the default connection to myorg-myacct-JDOE" in result.output


def test_a_rejected_warehouse_on_a_new_login_writes_nothing(
    runner, config_file, monkeypatch
):
    calls: list[dict] = []

    def connect(**kwargs):
        calls.append(kwargs)
        raise SnowflakeError(
            "The warehouse 'IdontExist' does not exist or not authorized."
        )

    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.snowflake.connector.connect",
        connect,
    )
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--warehouse", "IdontExist"],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    assert "IdontExist" in _panel_text(result.output)
    assert after == before
    assert len(calls) == 1
    assert "user" not in calls[0]
    assert calls[0]["warehouse"] == "IdontExist"
    assert calls[0]["validate_default_parameters"] is True
    assert calls[0]["client_store_temporary_credential"] is False


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


_SAVED_LOCAL_OAUTH = dedent(
    """\
    [connections.work]
    account = "myorg-myacct"
    user = "JDOE"
    database = "ANALYTICS"
    schema = "PUBLIC"
    warehouse = "COMPUTE_WH"
    role = "SYSADMIN"
    authenticator = "OAUTH_AUTHORIZATION_CODE"
    client_store_temporary_credential = true

    [connections.keypair]
    account = "other-acct"
    user = "JDOE"
    authenticator = "SNOWFLAKE_JWT"
    """
)


def test_a_second_login_updates_the_existing_local_oauth_connection(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "changed the default connection to work" in result.output
    assert set(written["connections"]) == {"work", "keypair"}
    connection = written["connections"]["work"]
    assert connection["user"] == "JDOE"
    assert connection["role"] == "SYSADMIN"
    assert connection["warehouse"] == "COMPUTE_WH"
    assert connection["database"] == "ANALYTICS"
    assert connection["schema"] == "PUBLIC"
    assert connection["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["default_connection_name"] == "work"
    assert calls[0]["account"] == "myorg-myacct"
    assert "user" not in calls[0]
    assert calls[1]["user"] == "JDOE"


def test_a_saved_connection_still_signs_in_so_another_user_can_be_chosen(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["SSMITH", "SSMITH"])
    host = "myorg-myacct.privatelink.snowflakecomputing.com"
    config = _SAVED_LOCAL_OAUTH.replace(
        'user = "JDOE"\n',
        f'user = "JDOE"\nhost = "{host}"\n',
        1,
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert "role" not in calls[0]
    assert calls[0]["host"] == host
    assert calls[0]["client_store_temporary_credential"] is False
    assert calls[1]["user"] == "SSMITH"
    assert "role" not in calls[1]
    assert "host" not in calls[1]
    assert calls[1]["client_store_temporary_credential"] is True
    assert set(written["connections"]) == {"work", "keypair", "myorg-myacct-SSMITH"}
    assert written["connections"]["work"]["user"] == "JDOE"
    assert written["connections"]["work"]["role"] == "SYSADMIN"
    assert written["connections"]["work"]["host"] == host
    ssmith = written["connections"]["myorg-myacct-SSMITH"]
    assert ssmith["user"] == "SSMITH"
    assert ssmith["account"] == "myorg-myacct"
    assert "role" not in ssmith
    assert "host" not in ssmith
    assert ssmith["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["default_connection_name"] == "myorg-myacct-SSMITH"


def test_a_bare_repeat_login_uses_the_host_stored_for_that_user(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    host = "myorg-myacct.privatelink.snowflakecomputing.com"
    config = _SAVED_LOCAL_OAUTH.replace(
        'user = "JDOE"\n',
        f'user = "JDOE"\nhost = "{host}"\n',
        1,
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "role" not in calls[0]
    assert calls[0]["host"] == host
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["role"] == "SYSADMIN"
    assert calls[1]["host"] == host
    connection = written["connections"]["work"]
    assert connection["host"] == host
    assert connection["user"] == "JDOE"
    assert connection["role"] == "SYSADMIN"
    assert set(written["connections"]) == {"work", "keypair"}


def test_a_saved_connection_signs_in_and_stores_the_given_host(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    host = "myorg-myacct.privatelink.snowflakecomputing.com"
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct", "--host", host]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[0]["host"] == host
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["host"] == host
    connection = written["connections"]["work"]
    assert connection["host"] == host
    assert connection["user"] == "JDOE"
    assert set(written["connections"]) == {"work", "keypair"}


def test_two_saved_users_on_one_account_still_sign_in(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    config = dedent(
        """\
        [connections.jdoe]
        account = "myorg-myacct"
        user = "JDOE"
        host = "myorg-myacct.privatelink.snowflakecomputing.com"
        role = "SYSADMIN"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true

        [connections.ssmith]
        account = "myorg-myacct"
        user = "SSMITH"
        role = "PUBLIC"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert "role" not in calls[0]
    assert "host" not in calls[0]
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["role"] == "SYSADMIN"
    assert calls[1]["host"] == "myorg-myacct.privatelink.snowflakecomputing.com"
    assert set(written["connections"]) == {"jdoe", "ssmith"}
    assert written["connections"]["jdoe"]["user"] == "JDOE"
    assert written["connections"]["jdoe"]["role"] == "SYSADMIN"
    assert (
        written["connections"]["jdoe"]["host"]
        == "myorg-myacct.privatelink.snowflakecomputing.com"
    )
    assert written["connections"]["ssmith"]["user"] == "SSMITH"
    assert written["connections"]["ssmith"]["role"] == "PUBLIC"
    assert "host" not in written["connections"]["ssmith"]
    assert written["default_connection_name"] == "jdoe"


@pytest.mark.parametrize(
    ("flag", "field"),
    [
        ("--warehouse", "warehouse"),
        ("--database", "database"),
        ("--schema", "schema"),
    ],
)
def test_a_rejected_warehouse_database_or_schema_is_not_stored(
    runner, config_file, monkeypatch, flag, field
):
    calls: list[dict] = []

    def connect(**kwargs):
        calls.append(kwargs)
        raise SnowflakeError(
            f"The {field} 'IdontExist' does not exist or not authorized."
        )

    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_oauth.snowflake.connector.connect",
        connect,
    )
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", flag, "IdontExist"],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    assert "IdontExist" in _panel_text(result.output)
    assert after == before
    assert len(calls) == 1
    assert "user" not in calls[0]
    assert calls[0][field] == "IdontExist"
    assert calls[0]["validate_default_parameters"] is True
    assert calls[0]["client_store_temporary_credential"] is False


def test_a_passed_role_or_warehouse_is_stored_after_sign_in(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                [
                    "auth",
                    "login",
                    "myorg-myacct",
                    "--role",
                    "ANALYST",
                    "--warehouse",
                    "NEW_WH",
                    "--database",
                    "NEW_DB",
                    "--schema",
                    "NEW_SCHEMA",
                ],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[0]["client_store_temporary_credential"] is False
    assert calls[1]["user"] == "JDOE"
    assert calls[1]["client_store_temporary_credential"] is True
    for call in calls:
        assert call["role"] == "ANALYST"
        assert call["warehouse"] == "NEW_WH"
        assert call["database"] == "NEW_DB"
        assert call["schema"] == "NEW_SCHEMA"
        assert call["validate_default_parameters"] is True
        assert "oauth_scope" not in call
    connection = written["connections"]["work"]
    assert connection["role"] == "ANALYST"
    assert connection["warehouse"] == "NEW_WH"
    assert connection["database"] == "NEW_DB"
    assert connection["schema"] == "NEW_SCHEMA"
    assert connection["user"] == "JDOE"
    assert set(written["connections"]) == {"work", "keypair"}


@pytest.mark.parametrize(
    ("flag", "removed", "kept_field", "kept_value"),
    [
        ("--clear-role", "role", "warehouse", "COMPUTE_WH"),
        ("--clear-warehouse", "warehouse", "role", "SYSADMIN"),
        ("--clear-database", "database", "schema", "PUBLIC"),
        ("--clear-schema", "schema", "database", "ANALYTICS"),
    ],
)
def test_clear_flags_remove_a_stored_connection_field(
    runner, config_file, monkeypatch, flag, removed, kept_field, kept_value
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct", flag]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[1]["user"] == "JDOE"
    if flag == "--clear-role":
        for call in calls:
            assert "role" not in call
    else:
        assert "role" not in calls[0]
        assert calls[1]["role"] == "SYSADMIN"
    connection = written["connections"]["work"]
    assert removed not in connection
    assert connection[kept_field] == kept_value
    assert connection["user"] == "JDOE"


@pytest.mark.parametrize(
    ("value_flag", "value", "clear_flag"),
    [
        ("--role", "ANALYST", "--clear-role"),
        ("--warehouse", "NEW_WH", "--clear-warehouse"),
        ("--database", "NEW_DB", "--clear-database"),
        ("--schema", "NEW_SCHEMA", "--clear-schema"),
    ],
)
def test_a_value_and_its_clear_flag_write_nothing(
    runner, config_file, monkeypatch, value_flag, value, clear_flag
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", value_flag, value, clear_flag],
            )
        after = path.read_text()

    assert result.exit_code == 2, result.output
    assert "incompatible" in result.output
    assert value_flag in result.output
    assert clear_flag in result.output
    assert after == before
    assert calls == []


def test_a_new_login_checks_warehouse_database_and_schema(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                [
                    "auth",
                    "login",
                    "myorg-myacct",
                    "--role",
                    "ANALYST",
                    "--warehouse",
                    "NEW_WH",
                    "--database",
                    "NEW_DB",
                    "--schema",
                    "NEW_SCHEMA",
                ],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    for call in calls:
        assert call["role"] == "ANALYST"
        assert "oauth_scope" not in call
        assert call["warehouse"] == "NEW_WH"
        assert call["database"] == "NEW_DB"
        assert call["schema"] == "NEW_SCHEMA"
        assert call["validate_default_parameters"] is True
    assert "user" not in calls[0]
    assert calls[0]["client_store_temporary_credential"] is False
    connection = written["connections"]["myorg-myacct-JDOE"]
    assert connection["role"] == "ANALYST"
    assert connection["warehouse"] == "NEW_WH"
    assert connection["database"] == "NEW_DB"
    assert connection["schema"] == "NEW_SCHEMA"


def test_a_sign_in_with_no_cached_session_uses_the_stored_role(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    assert "role" not in calls[0]
    assert calls[1]["role"] == "SYSADMIN"
    for call in calls:
        assert "oauth_scope" not in call
        assert "warehouse" not in call
        assert "database" not in call
        assert "schema" not in call
    assert written["connections"]["work"]["role"] == "SYSADMIN"
    assert written["connections"]["work"]["warehouse"] == "COMPUTE_WH"
    assert written["connections"]["work"]["database"] == "ANALYTICS"
    assert written["connections"]["work"]["schema"] == "PUBLIC"


def test_a_passed_role_replaces_the_stored_role_when_signing_in(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--role", "ANALYST"],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    for call in calls:
        assert call["role"] == "ANALYST"
        assert "oauth_scope" not in call
        assert "warehouse" not in call
        assert "database" not in call
        assert "schema" not in call
    connection = written["connections"]["work"]
    assert connection["role"] == "ANALYST"
    assert connection["warehouse"] == "COMPUTE_WH"
    assert connection["database"] == "ANALYTICS"
    assert connection["schema"] == "PUBLIC"


def test_clear_role_on_a_sign_in_does_not_pass_the_stored_role(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_SAVED_LOCAL_OAUTH) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct", "--clear-role"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert len(calls) == 2
    for call in calls:
        assert "role" not in call
        assert "warehouse" not in call
        assert "database" not in call
        assert "schema" not in call
    connection = written["connections"]["work"]
    assert "role" not in connection
    assert connection["warehouse"] == "COMPUTE_WH"
    assert connection["database"] == "ANALYTICS"
    assert connection["schema"] == "PUBLIC"


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
    kept = written["connections"]["myorg-myacct"]
    assert kept["database"] == "ANALYTICS"
    assert kept["schema"] == "PUBLIC"
    assert kept["warehouse"] == "COMPUTE_WH"
    assert kept["user"] == "OLD"
    assert kept["authenticator"] == "SNOWFLAKE_JWT"
    created = written["connections"]["myorg-myacct-JDOE"]
    assert created["user"] == "JDOE"
    assert created["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert "database" not in created
    assert "schema" not in created
    assert "warehouse" not in created
    assert written["default_connection_name"] == "myorg-myacct-JDOE"


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
    kept = written["connections"]["myorg-myacct"]
    assert kept["host"] == stored_host
    assert kept["user"] == "OLD"
    assert kept["database"] == "ANALYTICS"
    assert kept["authenticator"] == "SNOWFLAKE_JWT"
    created = written["connections"]["myorg-myacct-JDOE"]
    assert created["host"] == stored_host
    assert created["user"] == "JDOE"
    assert "database" not in created
    assert created["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["default_connection_name"] == "myorg-myacct-JDOE"


def test_a_host_flag_is_stored_on_the_new_connection(runner, config_file, monkeypatch):
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
    kept = written["connections"]["myorg-myacct"]
    assert kept["host"] == "myorg-myacct.privatelink.snowflakecomputing.com"
    assert kept["user"] == "OLD"
    assert kept["authenticator"] == "SNOWFLAKE_JWT"
    created = written["connections"]["myorg-myacct-JDOE"]
    assert created["host"] == replacement
    assert created["user"] == "JDOE"


@pytest.mark.parametrize("connector_v5", [False, True])
def test_login_stores_the_session_user_when_only_the_case_differs(
    runner, config_file, monkeypatch, connector_v5
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_adapter.IS_V5_DRIVER",
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
    assert written["connections"]["myorg-myacct-JDOE"]["user"] == "JDOE"
    assert "myorg-myacct-jdoe" not in written["connections"]


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

    name = f"{account}-JDOE"
    assert result.exit_code == 0, result.output
    assert f"changed the default connection to {name}" in result.output
    assert written["connections"][name]["user"] == "JDOE"
    assert written["default_connection_name"] == name


def test_a_quoted_session_user_that_differs_by_case_is_rejected_on_connector_5(
    runner, config_file, monkeypatch
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_adapter.IS_V5_DRIVER",
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
        "snowflake.cli._plugins.auth.connector_adapter.IS_V5_DRIVER",
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
    assert written["connections"]['myorg-myacct-"JDoe"']["user"] == '"JDoe"'


@pytest.mark.parametrize("connector_v5", [False, True])
def test_a_matching_quoted_session_user_keeps_that_spelling_in_the_name(
    runner, config_file, monkeypatch, connector_v5
):
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.connector_adapter.IS_V5_DRIVER",
        connector_v5,
    )
    _patch_sign_in(monkeypatch, ['"JDoe"', '"JDoe"'])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert written["connections"]['myorg-myacct-"JDoe"']["user"] == '"JDoe"'
    assert written["default_connection_name"] == 'myorg-myacct-"JDoe"'


def test_two_people_on_one_account_get_different_connection_names(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE", "SSMITH", "SSMITH"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            first = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
            second = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert set(written["connections"]) == {
        "keypair",
        "myorg-myacct-JDOE",
        "myorg-myacct-SSMITH",
    }
    assert written["connections"]["myorg-myacct-JDOE"]["user"] == "JDOE"
    assert written["connections"]["myorg-myacct-SSMITH"]["user"] == "SSMITH"
    assert written["connections"]["keypair"]["authenticator"] == "SNOWFLAKE_JWT"
    assert written["default_connection_name"] == "myorg-myacct-SSMITH"


def test_a_taken_derived_name_gets_a_short_suffix_and_is_printed(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    config = dedent(
        """\
        [connections.myorg-myacct-JDOE]
        account = "other-acct"
        user = "OLD"
        authenticator = "SNOWFLAKE_JWT"
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path), ["auth", "login", "myorg-myacct"]
            )
        written = tomlkit.parse(path.read_text())

    created = set(written["connections"]) - {"myorg-myacct-JDOE"}
    assert result.exit_code == 0, result.output
    assert len(created) == 1
    name = created.pop()
    suffix = name.removeprefix("myorg-myacct-JDOE-")
    assert name.startswith("myorg-myacct-JDOE-")
    assert len(suffix) == 4
    assert all(character in "0123456789abcdef" for character in suffix)
    assert f"changed the default connection to {name}" in result.output
    assert written["default_connection_name"] == name
    assert written["connections"]["myorg-myacct-JDOE"]["user"] == "OLD"
    assert (
        written["connections"]["myorg-myacct-JDOE"]["authenticator"] == "SNOWFLAKE_JWT"
    )
    assert written["connections"][name]["user"] == "JDOE"
    assert written["connections"][name]["account"] == "myorg-myacct"


def test_an_explicit_connection_name_is_what_gets_written(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "personal"],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "changed the default connection to personal" in result.output
    assert set(written["connections"]) == {"keypair", "personal"}
    assert written["connections"]["personal"]["user"] == "JDOE"
    assert written["connections"]["personal"]["account"] == "myorg-myacct"
    assert written["default_connection_name"] == "personal"


def test_a_blank_connection_name_writes_nothing(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "   "],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    assert "--connection-name" in result.output
    assert calls == []
    assert after == before


def test_an_explicit_name_owned_by_another_authenticator_stops_before_sign_in(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    with config_file(_OTHER_CONNECTION) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "keypair"],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    flat = _panel_text(result.output)
    assert "keypair" in flat
    assert "does not use Snowflake OAuth" in flat
    assert "Choose a different connection name." in flat
    assert calls == []
    assert after == before


def test_an_explicit_name_for_a_different_user_and_account_stops_after_sign_in(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    config = dedent(
        """\
        [connections.shared]
        account = "other-acct"
        user = "SSMITH"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true
        """
    )
    with config_file(config) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "shared"],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    flat = _panel_text(result.output)
    assert "shared" in flat
    assert "different user or account" in flat
    assert "Choose a different connection name." in flat
    assert len(calls) == 2
    assert calls[1]["user"] == "JDOE"
    assert after == before


def test_an_explicit_name_for_another_user_on_the_same_account_writes_nothing(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    config = dedent(
        """\
        [connections.ssmith]
        account = "myorg-myacct"
        user = "SSMITH"
        role = "PUBLIC"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true
        """
    )
    with config_file(config) as path:
        before = path.read_text()
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "ssmith"],
            )
        after = path.read_text()

    assert result.exit_code != 0, result.output
    flat = _panel_text(result.output)
    assert "ssmith" in flat
    assert "different user or account" in flat
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[1]["user"] == "JDOE"
    assert after == before


_RENAME_CONNECTION = dedent(
    """\
    default_connection_name = "work"

    [connections.work]
    account = "myorg-myacct"
    user = "JDOE"
    database = "ANALYTICS"
    schema = "PUBLIC"
    warehouse = "COMPUTE_WH"
    role = "SYSADMIN"
    authenticator = "OAUTH_AUTHORIZATION_CODE"
    client_store_temporary_credential = true

    [connections.keypair]
    account = "other-acct"
    user = "JDOE"
    authenticator = "SNOWFLAKE_JWT"
    """
)


def _tty(monkeypatch, interactive: bool) -> None:
    monkeypatch.setattr(
        "snowflake.cli._plugins.auth.manager.is_tty_interactive",
        lambda: interactive,
    )


def test_a_terminal_rename_moves_the_default_when_the_old_name_was_the_default(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    _tty(monkeypatch, True)
    with config_file(_RENAME_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "personal"],
                input="y\n",
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "Rename connection work to personal?" in result.output
    assert "[y/N]" in result.output
    assert len(calls) == 2
    assert "work" not in written["connections"]
    personal = written["connections"]["personal"]
    assert personal["user"] == "JDOE"
    assert personal["database"] == "ANALYTICS"
    assert personal["schema"] == "PUBLIC"
    assert personal["warehouse"] == "COMPUTE_WH"
    assert personal["role"] == "SYSADMIN"
    assert personal["authenticator"] == "OAUTH_AUTHORIZATION_CODE"
    assert written["connections"]["keypair"]["authenticator"] == "SNOWFLAKE_JWT"
    assert written["default_connection_name"] == "personal"
    assert "changed the default connection to personal" in result.output


def test_accepting_a_rename_sets_the_default_when_the_old_name_was_not(
    runner, config_file, monkeypatch
):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    _tty(monkeypatch, True)
    config = dedent(
        """\
        default_connection_name = "other"

        [connections.other]
        account = "other-acct"
        user = "SSMITH"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true

        [connections.work]
        account = "myorg-myacct"
        user = "JDOE"
        database = "ANALYTICS"
        role = "SYSADMIN"
        authenticator = "OAUTH_AUTHORIZATION_CODE"
        client_store_temporary_credential = true
        """
    )
    with config_file(config) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "personal"],
                input="y\n",
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "work" not in written["connections"]
    assert "personal" in written["connections"]
    assert written["connections"]["other"]["user"] == "SSMITH"
    assert written["default_connection_name"] == "personal"


def test_declining_a_rename_keeps_the_old_name(runner, config_file, monkeypatch):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    _tty(monkeypatch, True)
    with config_file(_RENAME_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "personal"],
                input="\n",
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "Rename connection work to personal?" in result.output
    assert "[y/N]" in result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[1]["user"] == "JDOE"
    assert set(written["connections"]) == {"work", "keypair"}
    assert written["connections"]["work"]["database"] == "ANALYTICS"
    assert written["connections"]["work"]["user"] == "JDOE"
    assert written["default_connection_name"] == "work"
    assert "changed the default connection to work" in result.output


def test_with_no_terminal_the_original_name_stays_and_login_says_so(
    runner, config_file, monkeypatch
):
    calls = _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    _tty(monkeypatch, False)
    with config_file(_RENAME_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "personal"],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "Kept connection name work." in result.output
    assert "A terminal is required to rename it to personal." in result.output
    assert "Rename connection" not in result.output
    assert len(calls) == 2
    assert "user" not in calls[0]
    assert calls[1]["user"] == "JDOE"
    assert set(written["connections"]) == {"work", "keypair"}
    assert written["connections"]["work"]["role"] == "SYSADMIN"
    assert written["default_connection_name"] == "work"


def test_the_same_connection_name_does_not_ask(runner, config_file, monkeypatch):
    _patch_sign_in(monkeypatch, ["JDOE", "JDOE"])
    _tty(monkeypatch, True)
    with config_file(_RENAME_CONNECTION) as path:
        with _enable_auth():
            result = runner.invoke_with_config_file(
                str(path),
                ["auth", "login", "myorg-myacct", "--connection-name", "work"],
            )
        written = tomlkit.parse(path.read_text())

    assert result.exit_code == 0, result.output
    assert "Rename connection" not in result.output
    assert "Kept connection name" not in result.output
    assert set(written["connections"]) == {"work", "keypair"}
    assert written["default_connection_name"] == "work"
