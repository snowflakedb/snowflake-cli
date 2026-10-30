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
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from unittest import mock

import pytest
from snowflake.cli._app.snow_connector import resolve_connection_parameters
from snowflake.cli._plugins.helpers.snowsl_vars_reader import check_env_vars
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import (
    config_init,
    get_connections_file,
)
from snowflake.cli.api.config_provider import reset_config_provider
from snowflake.cli.api.exceptions import MissingConfigurationError
from snowflake.connector.errors import ProgrammingError


@pytest.fixture
def legacy_config(config_file, monkeypatch):
    monkeypatch.delenv("SNOWFLAKE_CLI_CONFIG_V2_ENABLED", raising=False)

    @contextmanager
    def _load(text: str):
        with config_file(text) as path:
            get_cli_context().config_file_override = path
            config_init(path)
            reset_config_provider()
            yield path

    return _load


def test_generic_env_fills_gaps_and_does_not_override_connection_or_flag(
    legacy_config, monkeypatch
):
    with legacy_config(
        """
        [connections.demo]
        account = "from-file"
        user = "from-file"
        """
    ):
        _assert_generic_env_precedence(monkeypatch)


def _assert_generic_env_precedence(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_USER", "from-generic")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "from-generic")
    monkeypatch.setenv("SNOWSQL_USER", "from-snowsql")
    monkeypatch.setenv("SNOWFLAKE_REGION", "from-region")

    saved = resolve_connection_parameters(connection_name="demo")
    assert saved["user"] == "from-file"
    assert saved["account"] == "from-file"
    assert saved["warehouse"] == "from-generic"
    assert "region" not in saved

    monkeypatch.setenv("SNOWSQL_REGION", "eu-central-1")
    from_snowsql_region = resolve_connection_parameters(connection_name="demo")
    assert "region" not in from_snowsql_region
    assert from_snowsql_region["account"] == "from-file"

    flagged = resolve_connection_parameters(connection_name="demo", user="from-flag")
    assert flagged["user"] == "from-flag"

    temporary = resolve_connection_parameters(temporary_connection=True)
    assert temporary["user"] == "from-generic"
    assert temporary["warehouse"] == "from-generic"
    assert "account" not in temporary
    assert "from-snowsql" not in temporary.values()
    assert "from-snowsql" not in saved.values()


def test_saved_region_is_kept_and_snowsql_region_env_is_ignored(
    legacy_config, monkeypatch
):
    monkeypatch.delenv("SNOWSQL_REGION", raising=False)
    with legacy_config(
        """
        [connections.demo]
        account = "xy12345"
        region = "us-east-1"
        """
    ):
        saved = resolve_connection_parameters(connection_name="demo")
        assert saved["account"] == "xy12345"
        assert saved["region"] == "us-east-1"
        assert "host" not in saved

        monkeypatch.setenv("SNOWSQL_REGION", "eu-central-1")
        from_env = resolve_connection_parameters(connection_name="demo")
        assert from_env["region"] == "us-east-1"

        from_flag = resolve_connection_parameters(
            connection_name="demo", region="ap-southeast-2"
        )
        assert from_flag["region"] == "ap-southeast-2"


def test_snowsql_proxy_env_is_migration_only_and_current_proxy_env_still_works(
    legacy_config, monkeypatch
):
    from snowflake.connector.session_manager import (
        HttpConfig,
        SessionManager,
        SessionManagerFactory,
    )

    monkeypatch.setenv("SNOWSQL_PROXY_HOST", "old.example")
    monkeypatch.setenv("SNOWSQL_PROXY_PORT", "8443")
    monkeypatch.setenv("SNOWSQL_PROXY_USER", "old-user")
    monkeypatch.setenv("SNOWSQL_PROXY_PWD", "old-secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://current:secret@current.example:80")
    monkeypatch.setenv("HTTP_PROXY", "http://current:secret@current.example:80")

    with legacy_config(
        """
        [connections.demo]
        account = "xy12345"
        proxy_host = "from-file"
        proxy_port = "8080"
        proxy_user = "file-user"
        proxy_password = "file-secret"
        """
    ):
        saved = resolve_connection_parameters(connection_name="demo")
        assert saved["proxy_host"] == "from-file"
        assert saved["proxy_port"] == "8080"
        assert saved["proxy_user"] == "file-user"
        assert saved["proxy_password"] == "file-secret"

        flagged = resolve_connection_parameters(
            connection_name="demo", proxy_host="from-flag"
        )
        assert flagged["proxy_host"] == "from-flag"
        assert flagged["proxy_user"] == "file-user"

    with legacy_config(
        """
        [connections.demo]
        account = "xy12345"
        """
    ):
        current = resolve_connection_parameters(connection_name="demo")
        assert "proxy_host" not in current
        assert "proxy_port" not in current
        assert "proxy_user" not in current
        assert "proxy_password" not in current

    plain = SessionManagerFactory.get_manager(HttpConfig())
    assert type(plain) is SessionManager
    assert plain.make_session().trust_env is True


def test_snowsql_private_key_passphrase_env_is_migration_only(
    legacy_config, monkeypatch
):
    monkeypatch.setenv("SNOWSQL_PRIVATE_KEY_PASSPHRASE", "from-snowsql")
    with legacy_config(
        """
        [connections.demo]
        account = "xy12345"
        private_key_passphrase = "from-file"
        """
    ):
        saved = resolve_connection_parameters(connection_name="demo")
        assert saved["private_key_passphrase"] == "from-file"


def test_connection_specific_env_replaces_one_saved_key(legacy_config, monkeypatch):
    with legacy_config(
        """
        [connections.demo]
        account = "from-file"
        user = "from-file"
        private_key_file = "/from/file.p8"
        """
    ):
        _assert_connection_specific_env(monkeypatch)


def _assert_connection_specific_env(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_DEMO_USER", "from-specific")
    monkeypatch.setenv("SNOWFLAKE_USER", "from-generic")
    monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_DEMO_ACCOUNTNAME", "snowsql-style")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "/from/generic.p8")

    params = resolve_connection_parameters(connection_name="demo")

    assert params["user"] == "from-specific"
    assert params["account"] == "from-file"
    assert params["private_key_file"] == "/from/file.p8"

    overridden = resolve_connection_parameters(connection_name="demo", user="from-flag")
    assert overridden["user"] == "from-flag"


def test_connection_specific_env_does_not_create_a_connection(
    legacy_config, monkeypatch
):
    with legacy_config(
        """
        [connections.demo]
        account = "from-file"
        """
    ):
        _assert_missing_connection_is_not_created(monkeypatch)


def _assert_missing_connection_is_not_created(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_CONNECTIONS_MISSING_ACCOUNT", "created")

    with pytest.raises(MissingConfigurationError):
        resolve_connection_parameters(connection_name="missing")


def test_private_key_file_env_is_applied_before_private_key_path(
    legacy_config, monkeypatch
):
    with legacy_config(
        """
        [connections.demo]
        account = "from-file"
        """
    ):
        _assert_private_key_env_order(monkeypatch)


def _assert_private_key_env_order(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_FILE", "/from/file-env.p8")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/from/path-env.p8")

    on_saved = resolve_connection_parameters(connection_name="demo")
    assert on_saved["private_key_file"] == "/from/file-env.p8"

    temporary = resolve_connection_parameters(temporary_connection=True)
    assert temporary["private_key_file"] == "/from/file-env.p8"


def test_config_file_flag_does_not_move_connections_toml(
    runner, snowflake_home, tmp_path
):
    (snowflake_home / "connections.toml").write_text(
        '[from_connections_file]\naccount = "from-connections-toml"\n'
    )
    config = tmp_path / "elsewhere.toml"
    config.write_text(
        """
        default_connection_name = "from_connections_file"

        [connections.only_in_config]
        account = "from-config-file"
        """
    )

    result = runner.invoke_with_config_file(
        str(config),
        ["connection", "list", "--format", "JSON"],
    )

    assert result.exit_code == 0, result.output
    listed = json.loads(result.output)
    names = {row["connection_name"] for row in listed}
    assert names == {"from_connections_file"}
    assert listed[0]["is_default"] is True
    assert listed[0]["parameters"]["account"] == "from-connections-toml"
    assert get_connections_file() == snowflake_home / "connections.toml"
    assert get_connections_file().parent != config.parent


def test_snow_info_reports_config_file_in_existing_snowflake_home(tmp_path):
    home = tmp_path / "home"
    snowflake_home = home / "snow-home"
    snowflake_home.mkdir(parents=True)
    script = """
from snowflake.cli._app.cli_app import CliAppFactory
app = CliAppFactory().create_or_get_app()
app(["--info"])
"""
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["USERPROFILE"] = str(home)
    env["SNOWFLAKE_HOME"] = str(snowflake_home)
    env.pop("XDG_CONFIG_HOME", None)
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert completed.returncode == 0, completed.stderr
    start = completed.stdout.find("[")
    end = completed.stdout.rfind("]")
    payload = json.loads(completed.stdout[start : end + 1])
    reported = next(
        item["value"] for item in payload if item["key"] == "default_config_file_path"
    )
    assert reported == str(snowflake_home / "config.toml")


def test_rejected_snowsql_flags(runner, tmp_path):
    query = ["sql", "-q", "select 1", "--temporary-connection"]
    for flag in (
        ["--warehousename", "wh"],
        ["-o", "output_format=json"],
        ["--config", "cfg"],
    ):
        result = runner.invoke([*query, *flag])
        assert result.exit_code != 0, flag
        assert "No such option" in result.output


def test_snowsql_compatible_connection_aliases(runner, mock_cursor, tmp_path):
    key = tmp_path / "key.p8"
    key.write_text("not-a-key")
    with mock.patch(
        "snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string",
        return_value=(mock_cursor(["row"], []) for _ in range(1)),
    ):
        result = runner.invoke(
            [
                "sql",
                "-q",
                "select 1",
                "--temporary-connection",
                "--accountname",
                "acct",
                "--username",
                "usr",
                "--dbname",
                "db",
                "--schemaname",
                "sch",
                "--rolename",
                "rl",
                "--warehouse",
                "wh",
                "--host",
                "example.snowflakecomputing.com",
                "--port",
                "443",
                "--protocol",
                "https",
                "--private-key-path",
                str(key),
            ]
        )

    assert result.exit_code == 0, result.output
    ctx = get_cli_context().connection_context
    assert ctx.temporary_connection is True
    assert ctx.account == "acct"
    assert ctx.user == "usr"
    assert ctx.database == "db"
    assert ctx.schema == "sch"
    assert ctx.role == "rl"
    assert ctx.warehouse == "wh"
    assert ctx.host == "example.snowflakecomputing.com"
    assert ctx.port == 443
    assert ctx.protocol == "https"
    assert ctx.private_key_file == str(key)


@mock.patch("snowflake.cli._plugins.sql.manager.SqlExecutionMixin._execute_string")
def test_server_sql_error_exit_code_follows_enhanced_flag(
    mock_execute, runner, monkeypatch
):
    mock_execute.side_effect = ProgrammingError("SQL compilation error")

    plain = runner.invoke(["sql", "-q", "slect 1"])
    enhanced = runner.invoke(["sql", "-q", "slect 1", "--enhanced-exit-codes"])
    monkeypatch.setenv("SNOWFLAKE_ENHANCED_EXIT_CODES", "1")
    from_env = runner.invoke(["sql", "-q", "slect 1"])

    assert plain.exit_code == 1
    assert enhanced.exit_code == 5
    assert from_env.exit_code == 5


def test_enhanced_exit_codes_env_enables_the_flag_for_cli_commands(runner, monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_ENHANCED_EXIT_CODES", "1")

    result = runner.invoke(["connection", "list"])

    assert result.exit_code == 0, result.output
    assert get_cli_context().enhanced_exit_codes is True


def test_check_snowsql_env_vars_prints_suggestions_without_changing_environment(
    runner, monkeypatch
):
    monkeypatch.setenv("SNOWSQL_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWSQL_REGION", "us-west")
    monkeypatch.setenv("SNOWSQL_PROXY_HOST", "proxy.example")
    monkeypatch.setenv("EXIT_ON_ERROR", "true")
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    before = os.environ.copy()

    result = runner.invoke(["helpers", "check-snowsql-env-vars"])

    assert result.exit_code == 0, result.output
    assert os.environ == before
    assert "SNOWFLAKE_ACCOUNT" in result.output
    assert "SNOWFLAKE_REGION" not in result.output
    assert "SNOWSQL_PROXY_HOST" in result.output
    assert "SNOWFLAKE_ENHANCED_EXIT_CODES" not in result.output


def test_check_env_vars_does_not_mutate_its_input():
    from snowflake.cli._plugins.helpers.snowsl_vars_reader import (
        KNOWN_SNOWSQL_ENV_VARS,
    )

    expected = {
        "SNOWSQL_ACCOUNT": "SNOWFLAKE_ACCOUNT",
        "SNOWSQL_REGION": "SNOWFLAKE_ACCOUNT (account identifier, not the region name)",
        "SNOWSQL_PWD": "SNOWFLAKE_PASSWORD",
        "SNOWSQL_USER": "SNOWFLAKE_USER",
        "SNOWSQL_ROLE": "SNOWFLAKE_ROLE",
        "SNOWSQL_WAREHOUSE": "SNOWFLAKE_WAREHOUSE",
        "SNOWSQL_DATABASE": "SNOWFLAKE_DATABASE",
        "SNOWSQL_SCHEMA": "SNOWFLAKE_SCHEMA",
        "SNOWSQL_HOST": "SNOWFLAKE_HOST",
        "SNOWSQL_PORT": "SNOWFLAKE_PORT",
        "SNOWSQL_PROTOCOL": "SNOWFLAKE_PROTOCOL",
        "SNOWSQL_PROXY_HOST": "HTTPS_PROXY or HTTP_PROXY host",
        "SNOWSQL_PROXY_PORT": "HTTPS_PROXY or HTTP_PROXY port",
        "SNOWSQL_PROXY_USER": "HTTPS_PROXY or HTTP_PROXY username",
        "SNOWSQL_PROXY_PWD": "HTTPS_PROXY or HTTP_PROXY password",
        "SNOWSQL_PRIVATE_KEY_PASSPHRASE": "PRIVATE_KEY_PASSPHRASE",
    }
    variables = {name: "value" for name in expected}
    variables["EXIT_ON_ERROR"] = "true"
    original = dict(variables)

    discovered, unused, _summary = check_env_vars(variables)

    assert variables == original
    assert {row["Found"]: row["Suggested"] for row in discovered} == expected
    assert all("Found" in row for row in discovered)
    assert "SNOWFLAKE_REGION" not in {row["Suggested"] for row in discovered}
    assert "SNOWFLAKE_ENHANCED_EXIT_CODES" not in {
        row["Suggested"] for row in discovered
    }
    assert unused == []
    assert set(KNOWN_SNOWSQL_ENV_VARS) == set(expected)
