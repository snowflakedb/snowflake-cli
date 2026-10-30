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
from pathlib import Path

import pytest
import tomlkit
from snowflake.cli.api.secure_path import SecurePath

_TESTS_THAT_USE_THE_REAL_SNOWSQL_SEARCH_PATH = {
    "test_default_file_list_skips_missing_and_reads_rpm_file_last",
    "test_custom_files_merge_over_defaults_and_rpm_file_wins",
    "test_workspace_replaces_only_the_user_config_paths",
    "test_blank_workspace_keeps_the_home_config_paths",
}

DEFAULT_SNOWSQL_FILES = (
    "/etc/snowsql.cnf",
    "/etc/snowflake/snowsql.cnf",
    "/usr/local/etc/snowsql.cnf",
    str(Path.home() / ".snowsql.cnf"),
    str(Path.home() / ".snowsql" / "config"),
    "/usr/lib64/snowflake/snowsql/config",
)


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    path.chmod(0o600)
    return path


def _load(path: Path) -> dict:
    return tomlkit.loads(path.read_text()).unwrap()


@pytest.fixture(autouse=True)
def isolate_snowsql_search_path(monkeypatch, request):
    """Read only the files the test passes, not this machine's SnowSQL configs."""
    if request.node.name in _TESTS_THAT_USE_THE_REAL_SNOWSQL_SEARCH_PATH:
        return
    from snowflake.cli._plugins.helpers import commands as helper_commands

    monkeypatch.setattr(
        helper_commands,
        "_snowsql_config_files",
        lambda custom_files: list(custom_files or []),
    )


def test_default_file_list_skips_missing_and_reads_rpm_file_last(runner, monkeypatch):
    monkeypatch.delenv("WORKSPACE", raising=False)
    seen: list[str] = []
    original_exists = SecurePath.exists

    def exists(self):
        text = str(self.path)
        if "snowsql" in text.lower():
            seen.append(text)
            return False
        return original_exists(self)

    monkeypatch.setattr(SecurePath, "exists", exists)
    result = runner.invoke(["helpers", "import-snowsql-connections"])

    assert result.exit_code == 0, result.output
    expected_paths = [str(Path(path)) for path in DEFAULT_SNOWSQL_FILES]
    assert seen == expected_paths
    assert seen[-1] == str(Path("/usr/lib64/snowflake/snowsql/config"))
    for path in expected_paths:
        assert (
            f"SnowSQL config file [{path}] does not exist. Skipping." in result.output
        )
    assert not any(
        "password1234" in path or path.endswith("/snowsql/snowsql.cnf") for path in seen
    )


def test_custom_snowsql_config_file_must_exist(runner, tmp_path):
    present = _write(tmp_path / "present.cnf", "[connections]\naccountname=acct\n")
    missing = tmp_path / "missing.cnf"
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(missing),
            "--snowsql-config-file",
            str(present),
        ],
    )

    assert result.exit_code != 0
    assert "does not exist" in result.output.lower()
    assert _load(config) == {}


def test_later_file_replaces_same_key_and_keeps_earlier_keys(runner, tmp_path):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.example]
        username=user1
        accountname=from-earlier
        password=keep-me
        """,
    )
    later = _write(
        tmp_path / "later.cnf",
        """
        [connections.example]
        username=user2
        databasename=mydb
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(later),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["example"]
    assert saved["user"] == "user2"
    assert saved["account"] == "from-earlier"
    assert saved["password"] == "keep-me"
    assert saved["database"] == "mydb"


def test_import_copies_connection_sections_without_unnamed_keys(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [variables]
        foo=bar

        [options]
        exit_on_error=true

        [connections]
        AccountName=myorg-myaccount
        username=default-user
        dbname=db-from-dbname

        [connections.named]
        username=named-user
        databasename=db-from-databasename
        schemaname=public
        warehousename=wh
        rolename=rl
        private_key_path=/path/to/key.p8
        authenticator=SNOWFLAKE_JWT
        login_timeout=30

        [connections.empty]
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert "variables" not in loaded
    assert "options" not in loaded
    assert set(loaded["connections"]) == {"default", "named"}
    assert loaded["connections"]["default"] == {
        "account": "myorg-myaccount",
        "user": "default-user",
        "database": "db-from-dbname",
    }
    assert loaded["connections"]["named"] == {
        "user": "named-user",
        "database": "db-from-databasename",
        "schema": "public",
        "warehouse": "wh",
        "role": "rl",
        "private_key_file": "/path/to/key.p8",
        "authenticator": "SNOWFLAKE_JWT",
        "login_timeout": 30,
    }
    assert loaded["default_connection_name"] == "default"


def test_named_default_without_unnamed_section_is_not_the_cli_default(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.default]
        username=named-only
        accountname=acct
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert loaded["connections"]["default"]["user"] == "named-only"
    assert "default_connection_name" not in loaded


def test_name_collision_renames_unnamed_section_without_replacing_named(
    runner, tmp_path
):
    """The unnamed section keeps the keys SnowSQL uses when ``-c`` is omitted.

    The named ``[connections.default]`` section is saved separately. The CLI
    default is the prompted name of the unnamed section.
    """
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=from-unnamed
        username=unnamed-user

        [connections.default]
        accountname=from-named
        username=named-user
        password=named-password
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="renamed\n",
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert loaded["connections"]["default"] == {
        "account": "from-named",
        "user": "named-user",
        "password": "named-password",
    }
    assert loaded["connections"]["renamed"] == {
        "account": "from-unnamed",
        "user": "unnamed-user",
    }
    assert loaded["default_connection_name"] == "renamed"


def test_declining_overwrite_leaves_existing_connection_and_default_unchanged(
    runner, tmp_path
):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.default]
        username=from-snowsql
        accountname=from-snowsql
        """,
    )
    config = _write(
        tmp_path / "config.toml",
        """
        default_connection_name = "keep-me"

        [connections.default]
        user = "original"
        account = "original"
        """,
    )

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="\n",
    )

    assert result.exit_code == 0, result.output
    assert "[y/N]" in result.output
    loaded = _load(config)
    assert loaded["connections"]["default"]["user"] == "original"
    assert loaded["connections"]["default"]["account"] == "original"
    assert loaded["default_connection_name"] == "keep-me"


def test_confirming_overwrite_replaces_existing_connection(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.default]
        username=from-snowsql
        accountname=from-snowsql
        """,
    )
    config = _write(
        tmp_path / "config.toml",
        """
        [connections.default]
        user = "original"
        account = "original"
        """,
    )

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="y\n",
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert loaded["connections"]["default"]["user"] == "from-snowsql"
    assert loaded["connections"]["default"]["account"] == "from-snowsql"
    assert "default_connection_name" not in loaded


def test_connections_toml_receives_top_level_sections_and_default_stays_in_config(
    runner, snowflake_home, tmp_path
):
    connections = _write(snowflake_home / "connections.toml", "")
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.default]
        username=user2
        accountname=myorg-myaccount
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    assert _load(connections) == {
        "default": {"user": "user2", "account": "myorg-myaccount"}
    }
    assert "connections" not in _load(connections)
    config_data = _load(config)
    assert "connections" not in config_data
    assert "default_connection_name" not in config_data


def test_custom_files_merge_over_defaults_and_rpm_file_wins(
    runner, tmp_path, monkeypatch
):
    from snowflake.cli._plugins.helpers import commands as helper_commands

    base = _write(
        tmp_path / "base.cnf",
        """
        [connections.example]
        accountname=from-default
        username=from-default
        password=from-default
        """,
    )
    custom = _write(
        tmp_path / "custom.cnf",
        """
        [connections.example]
        username=from-custom
        """,
    )
    rpm = _write(
        tmp_path / "rpm.cnf",
        """
        [connections.example]
        password=from-rpm
        """,
    )

    def files(custom_files):
        return [base, *(custom_files or []), rpm]

    monkeypatch.setattr(helper_commands, "_snowsql_config_files", files)
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(custom),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["example"]
    assert saved["account"] == "from-default"
    assert saved["user"] == "from-custom"
    assert saved["password"] == "from-rpm"


def test_short_snowsql_key_wins_over_long_alias_regardless_of_order(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.long-first]
        accountname=from-long
        account=from-short
        username=from-long
        user=from-short
        dbname=from-dbname
        databasename=from-databasename
        database=from-database
        warehousename=from-long
        warehouse=from-short
        schemaname=from-long
        schema=from-short
        rolename=from-long
        role=from-short

        [connections.alias-only]
        databasename=from-databasename
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["long-first"]
    assert saved["account"] == "from-short"
    assert saved["user"] == "from-short"
    assert saved["database"] == "from-database"
    assert saved["warehouse"] == "from-short"
    assert saved["schema"] == "from-short"
    assert saved["role"] == "from-short"
    assert "accountname" not in saved
    assert "databasename" not in saved
    assert _load(config)["connections"]["alias-only"]["database"] == "from-databasename"


def test_text_stays_string_and_connector_int_and_bool_are_coerced(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        password=123456
        login_timeout=30
        client_prefetch_threads=4
        ocsp_fail_open=False
        client_session_keep_alive=false
        port=443
        secret=100%sure

        [connections.unparsed]
        login_timeout=30s
        ocsp_fail_open=maybe
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["password"] == "123456"
    assert isinstance(saved["password"], str)
    assert saved["login_timeout"] == 30
    assert type(saved["login_timeout"]) is int
    assert saved["client_prefetch_threads"] == 4
    assert type(saved["client_prefetch_threads"]) is int
    assert saved["ocsp_fail_open"] is False
    assert saved["client_session_keep_alive"] is False
    assert saved["port"] == "443"
    assert isinstance(saved["port"], str)
    assert saved["secret"] == "100%sure"
    unparsed = _load(config)["connections"]["unparsed"]
    assert unparsed["login_timeout"] == "30s"
    assert unparsed["ocsp_fail_open"] == "maybe"


def test_file_private_key_passphrase_is_imported(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        private_key_path=/path/to/key.p8
        private_key_passphrase=from-file
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["private_key_file"] == "/path/to/key.p8"
    assert saved["authenticator"] == "SNOWFLAKE_JWT"
    assert saved["private_key_passphrase"] == "from-file"


def test_connector_private_key_file_pwd_is_imported_as_passphrase(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        private_key_path=/path/to/key.p8
        private_key_file_pwd=from-connector-name
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["private_key_passphrase"] == "from-connector-name"
    assert "private_key_file_pwd" not in saved


def test_connectivity_keys_are_copied_through(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        host=example.snowflakecomputing.com
        port=443
        protocol=https
        proxy_host=proxy.example
        proxy_port=8443
        proxy_user=proxy-user
        proxy_password=proxy-secret
        proxy_pwd=ignored-when-password-is-set
        token=token-value
        workload_identity_provider=AWS
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["host"] == "example.snowflakecomputing.com"
    assert saved["port"] == "443"
    assert saved["protocol"] == "https"
    assert saved["proxy_host"] == "proxy.example"
    assert saved["proxy_port"] == "8443"
    assert saved["proxy_user"] == "proxy-user"
    assert saved["proxy_password"] == "proxy-secret"
    assert "proxy_pwd" not in saved
    assert saved["token"] == "token-value"
    assert saved["workload_identity_provider"] == "AWS"


def test_region_is_preserved_without_rewriting_host(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.locator]
        accountname=xy12345
        region=us-east-1

        [connections.org]
        accountname=myorg-myaccount
        region=us-east-1

        [connections.explicit]
        accountname=xy12345
        region=us-west-2
        host=example.snowflakecomputing.com
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    assert loaded["locator"]["account"] == "xy12345"
    assert loaded["locator"]["region"] == "us-east-1"
    assert "host" not in loaded["locator"]
    assert loaded["org"]["region"] == "us-east-1"
    assert "host" not in loaded["org"]
    assert loaded["explicit"]["host"] == "example.snowflakecomputing.com"
    assert loaded["explicit"]["region"] == "us-west-2"


def test_short_key_in_an_earlier_file_wins_over_a_later_long_alias(runner, tmp_path):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.example]
        account=from-short
        username=from-long
        """,
    )
    later = _write(
        tmp_path / "later.cnf",
        """
        [connections.example]
        accountname=from-long
        user=from-short
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(later),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["example"]
    assert saved["account"] == "from-short"
    assert saved["user"] == "from-short"
    assert "accountname" not in saved
    assert "username" not in saved


def test_later_private_key_does_not_replace_an_earlier_authenticator(runner, tmp_path):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.example]
        accountname=acct
        authenticator=oauth
        """,
    )
    later = _write(
        tmp_path / "later.cnf",
        """
        [connections.example]
        private_key_path=/path/to/key.p8
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(later),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["example"]
    assert saved["authenticator"] == "oauth"
    assert "private_key_file" not in saved
    assert "private_key_passphrase" not in saved
    assert "omits the private key" in result.output


def test_named_connection_does_not_inherit_authenticator(runner, tmp_path):
    """The unnamed authenticator stays on the default connection.

    ``connections.keyed`` does not receive it, so its private key is kept.
    """
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        authenticator=oauth

        [connections.keyed]
        private_key_path=/path/to/key.p8
        private_key_file_pwd=secret
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    assert loaded["default"]["authenticator"] == "oauth"
    assert "private_key_file" not in loaded["default"]
    saved = loaded["keyed"]
    assert saved["authenticator"] == "SNOWFLAKE_JWT"
    assert saved["private_key_file"] == "/path/to/key.p8"
    assert saved["private_key_passphrase"] == "secret"
    assert "account" not in saved
    assert "omits the private key" not in result.output


def test_named_connection_does_not_inherit_unnamed_keys(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        account=from-unnamed
        warehousename=from-unnamed
        username=unnamed-user

        [connections.named]
        accountname=from-named
        username=named-user
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert loaded["connections"]["named"] == {
        "account": "from-named",
        "user": "named-user",
    }
    assert loaded["connections"]["default"]["account"] == "from-unnamed"
    assert loaded["default_connection_name"] == "default"


def test_pwd_is_saved_as_password_and_password_wins(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.both]
        accountname=acct
        password=from-password
        pwd=from-pwd

        [connections.pwd-only]
        accountname=acct
        pwd=from-pwd
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    assert loaded["both"]["password"] == "from-password"
    assert "pwd" not in loaded["both"]
    assert loaded["pwd-only"]["password"] == "from-pwd"
    assert "pwd" not in loaded["pwd-only"]


def test_quoted_account_is_imported_without_quotes(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname = "acct"
        password = "123456"
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["account"] == "acct"
    assert saved["password"] == "123456"
    assert isinstance(saved["password"], str)


def test_private_key_path_wins_over_private_key_file(runner, tmp_path):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.example]
        accountname=acct
        private_key_file=/from-file
        """,
    )
    later = _write(
        tmp_path / "later.cnf",
        """
        [connections.example]
        private_key_path=/from-path
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(later),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["example"]
    assert saved["private_key_file"] == "/from-path"
    assert saved["authenticator"] == "SNOWFLAKE_JWT"


def test_blank_value_is_unset_and_does_not_block_a_real_password(runner, tmp_path):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.cleared]
        accountname=acct
        password=secret
        """,
    )
    later = _write(
        tmp_path / "later.cnf",
        """
        [connections]
        accountname=acct
        password=from-unnamed
        warehousename=wh
        token=

        [connections.cleared]
        password=

        [connections.omitted]
        username=named-user

        [connections.explicit-blank]
        username=blank-user
        password=
        login_timeout=0
        client_session_keep_alive=False
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(later),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    assert loaded["default"]["password"] == "from-unnamed"
    assert loaded["default"]["account"] == "acct"
    assert "token" not in loaded["default"]
    assert "password" not in loaded["cleared"]
    assert loaded["cleared"]["account"] == "acct"
    assert loaded["omitted"] == {"user": "named-user"}
    assert "password" not in loaded["explicit-blank"]
    assert loaded["explicit-blank"]["user"] == "blank-user"
    assert "account" not in loaded["explicit-blank"]
    assert "warehouse" not in loaded["explicit-blank"]
    assert loaded["explicit-blank"]["login_timeout"] == 0
    assert loaded["explicit-blank"]["client_session_keep_alive"] is False


def test_unreadable_snowsql_file_fails_before_writing(runner, tmp_path, monkeypatch):
    earlier = _write(
        tmp_path / "earlier.cnf",
        """
        [connections.example]
        accountname=from-earlier
        username=user1
        """,
    )
    unreadable = _write(
        tmp_path / "unreadable.cnf",
        """
        [connections.example]
        username=from-unreadable
        """,
    )
    config = _write(tmp_path / "config.toml", "")
    real_open = Path.open

    def open_denying_unreadable(path, *args, **kwargs):
        if Path(path) == unreadable:
            raise PermissionError(13, "Permission denied", str(path))
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", open_denying_unreadable)

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(earlier),
            "--snowsql-config-file",
            str(unreadable),
        ],
    )

    assert result.exit_code != 0
    assert str(unreadable) in result.output
    assert "Permission denied" in result.output
    assert "successfully imported" not in result.output.lower()
    assert _load(config) == {}


def test_unnamed_private_key_warning_uses_the_name_saved_after_collision(
    runner, tmp_path
):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=acct
        authenticator=oauth
        private_key_path=/path/to/key.p8

        [connections.default]
        accountname=other
        username=named-user
        private_key_path=
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="renamed\n",
    )

    assert result.exit_code == 0, result.output
    assert "Connection [renamed] keeps authenticator 'oauth'" in result.output
    assert "Connection [default] keeps authenticator" not in result.output
    saved = _load(config)["connections"]["renamed"]
    assert saved["authenticator"] == "oauth"
    assert "private_key_file" not in saved
    assert _load(config)["default_connection_name"] == "renamed"


def test_malformed_snowsql_file_is_a_command_error(runner, tmp_path):
    duplicate = _write(
        tmp_path / "duplicate.cnf",
        """
        [connections]
        accountname=acct
        accountname=other
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(duplicate),
        ],
    )

    assert result.exit_code != 0
    assert str(duplicate) in result.output
    assert "Traceback" not in result.output
    assert "successfully imported" not in result.output.lower()
    assert _load(config) == {}


def test_undecodable_snowsql_file_is_a_command_error(runner, tmp_path):
    undecodable = tmp_path / "undecodable.cnf"
    # 0x81 is not valid in UTF-8 or Windows-1252. 0xFF is ÿ in Windows-1252,
    # so the import would succeed on Windows.
    undecodable.write_bytes(b"[connections]\naccountname=\x81\n")
    undecodable.chmod(0o600)
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(undecodable),
        ],
    )

    assert result.exit_code != 0
    assert str(undecodable) in result.output
    assert "Traceback" not in result.output
    assert "successfully imported" not in result.output.lower()
    assert _load(config) == {}


def test_section_with_no_remaining_values_is_not_saved(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=

        [connections.empty]

        [connections.placeholder]
        password=

        [connections.kept]
        accountname=acct
        username=named-user
        password=
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert set(loaded["connections"]) == {"kept"}
    assert "default_connection_name" not in loaded
    assert loaded["connections"]["kept"]["account"] == "acct"
    assert loaded["connections"]["kept"]["user"] == "named-user"
    assert "password" not in loaded["connections"]["kept"]
    assert "Empty connection configuration [connections.placeholder]" in result.output


def test_parser_error_does_not_repeat_the_source_line(runner, tmp_path):
    secret = "hunter2-should-not-appear"
    broken = _write(
        tmp_path / "broken.cnf",
        f"""
        [connections]
        accountname=acct
        {secret}
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(broken),
        ],
    )

    assert result.exit_code != 0
    assert str(broken) in result.output
    # The error panel wraps a long path, so the border can sit between words.
    shown = " ".join(result.output.replace("|", " ").split())
    assert "the file could not be parsed" in shown
    assert secret not in result.output
    assert "Traceback" not in result.output
    assert _load(config) == {}


def test_workspace_replaces_only_the_user_config_paths(runner, monkeypatch, tmp_path):
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("WORKSPACE", str(workspace))
    seen: list[str] = []
    original_exists = SecurePath.exists

    def exists(self):
        text = str(self.path)
        if "snowsql" in text.lower() or "workspace" in text:
            seen.append(text)
            return False
        return original_exists(self)

    monkeypatch.setattr(SecurePath, "exists", exists)
    result = runner.invoke(["helpers", "import-snowsql-connections"])

    assert result.exit_code == 0, result.output
    assert str(workspace / ".snowsql.cnf") in seen
    assert str(workspace / ".snowsql" / "config") in seen
    assert str(Path.home() / ".snowsql.cnf") not in seen
    assert str(Path.home() / ".snowsql" / "config") not in seen
    assert str(Path("/etc/snowsql.cnf")) in seen
    assert seen[-1] == str(Path("/usr/lib64/snowflake/snowsql/config"))


def test_options_fill_omitted_connection_settings_and_connection_wins(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [options]
        exit_on_error=true
        log_level=DEBUG
        login_timeout=15
        insecure_mode=yes
        client_session_keep_alive=on
        ocsp_fail_open=off
        client_store_temporary_credential=no

        [connections]
        accountname=acct

        [connections.named]
        accountname=named
        login_timeout=0
        insecure_mode=false

        [connections.blank-only]
        accountname=
        username=
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    assert loaded["default"]["login_timeout"] == 15
    assert loaded["default"]["insecure_mode"] is True
    assert loaded["default"]["client_session_keep_alive"] is True
    assert loaded["default"]["ocsp_fail_open"] is False
    assert loaded["default"]["client_store_temporary_credential"] is False
    assert "exit_on_error" not in loaded["default"]
    assert "log_level" not in loaded["default"]
    assert loaded["named"]["login_timeout"] == 0
    assert type(loaded["named"]["login_timeout"]) is int
    assert loaded["named"]["insecure_mode"] is False
    assert loaded["named"]["client_session_keep_alive"] is True
    assert loaded["named"]["account"] == "named"
    assert "user" not in loaded["named"]
    assert "blank-only" not in loaded


def test_quotes_comments_and_commas_follow_snowsql(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname = "acct"
        username = myacct # prod
        password = "my#pass"
        token = a, b
        lonely = ,
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    saved = _load(config)["connections"]["default"]
    assert saved["account"] == "acct"
    assert saved["user"] == "myacct"
    assert saved["password"] == "my#pass"
    assert saved["token"] == "a, b"
    assert saved["lonely"] == ","


def test_default_name_collision_stops_after_three_prompts(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=from-unnamed

        [connections.default]
        accountname=from-named
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="default\ndefault\ndefault\n",
    )

    assert result.exit_code != 0
    assert "Could not choose a name" in result.output
    assert "Traceback" not in result.output
    assert _load(config) == {}


def test_default_name_collision_accepts_a_later_free_name(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=from-unnamed

        [connections.default]
        accountname=from-named
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="default\ndefault\nrenamed\n",
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)
    assert loaded["default_connection_name"] == "renamed"
    assert loaded["connections"]["renamed"]["account"] == "from-unnamed"
    assert loaded["connections"]["default"]["account"] == "from-named"


def test_default_name_collision_counts_blank_lines(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections]
        accountname=from-unnamed

        [connections.default]
        accountname=from-named
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="\n\n\n",
    )

    assert result.exit_code != 0
    assert "Could not choose a name" in result.output
    assert "Traceback" not in result.output
    assert _load(config) == {}


def test_collision_prompt_drops_control_characters(monkeypatch):
    from snowflake.cli._plugins.helpers.commands import (
        _validate_imported_default_connection_name,
    )

    prompts: list[tuple] = []

    def prompt(text, default=None, show_default=True):
        prompts.append((text, default, show_default))
        return "free-name"

    monkeypatch.setattr("snowflake.cli._plugins.helpers.commands.typer.prompt", prompt)
    name = _validate_imported_default_connection_name(
        "bad\rname",
        {"bad\rname": {"account": "x"}},
    )

    assert name == "free-name"
    assert len(prompts) == 1
    shown, default, show_default = prompts[0]
    assert "\r" not in shown
    assert "badname" in shown
    assert default == ""
    assert show_default is False


def test_blank_workspace_keeps_the_home_config_paths(runner, monkeypatch):
    original_exists = SecurePath.exists

    for value in ("", "   ", "\t"):
        seen: list[str] = []

        def exists(self, seen=seen):
            text = str(self.path)
            if "snowsql" in text.lower():
                seen.append(text)
                return False
            return original_exists(self)

        monkeypatch.setenv("WORKSPACE", value)
        monkeypatch.setattr(SecurePath, "exists", exists)
        result = runner.invoke(["helpers", "import-snowsql-connections"])

        assert result.exit_code == 0, result.output
        assert str(Path.home() / ".snowsql.cnf") in seen
        assert str(Path.home() / ".snowsql" / "config") in seen
        assert str(Path(value) / ".snowsql" / "config") not in seen


def test_blank_short_alias_falls_through_to_the_long_name(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.example]
        user=
        username=alice
        account=
        accountname=acct
        password=
        pwd=secret
        private_key_path=
        private_key_file=/keys/long.p8

        [connections.whitespace]
        user=\t
        username=bob
        accountname=acct

        [connections.all-blank]
        user=
        username=
        accountname=acct
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    example = loaded["example"]
    assert example["user"] == "alice"
    assert example["account"] == "acct"
    assert example["password"] == "secret"
    assert example["private_key_file"] == "/keys/long.p8"
    assert example["authenticator"] == "SNOWFLAKE_JWT"
    assert "username" not in example
    assert "pwd" not in example
    assert "private_key_path" not in example
    whitespace = loaded["whitespace"]
    assert whitespace["user"] == "bob"
    assert "user" not in loaded["all-blank"]
    assert loaded["all-blank"]["account"] == "acct"


def test_oauth_and_pooling_booleans_are_coerced(runner, tmp_path):
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        """
        [connections.example]
        accountname=acct
        oauth_disable_pkce=false
        oauth_enable_refresh_tokens=no
        oauth_enable_single_use_refresh_tokens=off
        disable_request_pooling=0
        oauth_client_id=client

        [connections.unparsed]
        accountname=acct
        oauth_disable_pkce=maybe
        """,
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    loaded = _load(config)["connections"]
    example = loaded["example"]
    assert example["oauth_disable_pkce"] is False
    assert example["oauth_enable_refresh_tokens"] is False
    assert example["oauth_enable_single_use_refresh_tokens"] is False
    assert example["disable_request_pooling"] is False
    assert example["oauth_client_id"] == "client"
    assert loaded["unparsed"]["oauth_disable_pkce"] == "maybe"


def test_printed_names_drop_controls_and_the_saved_key_stays_raw(runner, tmp_path):
    # ConfigObj splits on CR, so a carriage return never becomes a section name.
    # Backspace and ANSI do, and both can rewrite the overwrite prompt.
    raw_name = "legit\x08Saved"
    snowsql = _write(
        tmp_path / "snowsql.cnf",
        f"[connections.{raw_name}]\n"
        "accountname=acct\n"
        "username=alice\n"
        "authenticator=external\x1b[31mbrowser\n"
        "private_key_path=/tmp/key.p8\n",
    )
    config = _write(tmp_path / "config.toml", "")

    result = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "\x08" not in result.output
    assert "\x1b" not in result.output
    assert "legitSaved" in result.output
    assert "externalbrowser" in result.output
    saved = _load(config)["connections"][raw_name]
    assert saved["user"] == "alice"
    assert saved["authenticator"] == "external\x1b[31mbrowser"
    assert "private_key_file" not in saved

    again = runner.invoke_with_config_file(
        config,
        [
            "helpers",
            "import-snowsql-connections",
            "--snowsql-config-file",
            str(snowsql),
        ],
        input="n\n",
    )

    assert again.exit_code == 0, again.output
    assert "\x08" not in again.output
    assert "\x1b" not in again.output
    assert "Connection 'legitSaved' already exists" in again.output
    assert _load(config)["connections"][raw_name]["user"] == "alice"


def test_snowsql_config_file_help_says_it_does_not_replace_the_default_list(runner):
    result = runner.invoke(["helpers", "import-snowsql-connections", "--help"])

    assert result.exit_code == 0, result.output
    collapsed = " ".join(result.output.replace("|", " ").split())
    assert collapsed.count("does not replace the default file list") == 2
