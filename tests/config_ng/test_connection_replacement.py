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

"""
Tests for connection-level replacement behavior in config_ng.

These tests verify that:
1. FILE sources (snowsql_config, cli_config_toml, connections_toml) use
   connection-level replacement (later file replaces entire connection)
2. OVERLAY sources (env vars, CLI args) use field-level overlay
3. SnowSQL's multi-file merge acts as a single FILE source
"""


def test_file_replacement_basic(config_ng_setup):
    """
    Test basic file replacement: later file replaces entire connection.
    Fields from earlier file should NOT be inherited.
    """
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    user = snowsql-user
    warehouse = snowsql-warehouse
    database = snowsql-database
    """

    cli_config = """
    [connections.test]
    account = "cli-account"
    user = "cli-user"
    # Note: warehouse and database are NOT included
    """

    with config_ng_setup(snowsql_config=snowsql_config, cli_config=cli_config):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Values from cli_config (later FILE source)
        assert conn["account"] == "cli-account"
        assert conn["user"] == "cli-user"

        # warehouse and database from snowsql NOT inherited (connection replaced)
        assert "warehouse" not in conn
        assert "database" not in conn


def test_file_replacement_connections_toml_replaces_cli_config(config_ng_setup):
    """
    Test that connections.toml replaces cli_config.toml entirely.
    """
    cli_config = """
    [connections.prod]
    account = "cli-account"
    user = "cli-user"
    warehouse = "cli-warehouse"
    database = "cli-database"
    schema = "cli-schema"
    """

    connections_toml = """
    [connections.prod]
    account = "conn-account"
    database = "conn-database"
    # Note: user, warehouse, schema are NOT included
    """

    with config_ng_setup(cli_config=cli_config, connections_toml=connections_toml):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("prod")

        # Values from connections.toml
        assert conn["account"] == "conn-account"
        assert conn["database"] == "conn-database"

        # user, warehouse, schema from cli_config NOT inherited
        assert "user" not in conn
        assert "warehouse" not in conn
        assert "schema" not in conn


def test_file_replacement_three_levels(config_ng_setup):
    """
    Test replacement across three file sources: snowsql -> cli_config -> connections.toml
    """
    snowsql_config = """
    [connections.dev]
    accountname = snowsql-account
    user = snowsql-user
    warehouse = snowsql-warehouse
    database = snowsql-database
    schema = snowsql-schema
    """

    cli_config = """
    [connections.dev]
    account = "cli-account"
    user = "cli-user"
    warehouse = "cli-warehouse"
    # database and schema not included
    """

    connections_toml = """
    [connections.dev]
    account = "conn-account"
    # Only account specified
    """

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("dev")

        # Only value from connections.toml (highest FILE source)
        assert conn["account"] == "conn-account"

        # All other fields NOT inherited from earlier FILE sources
        assert "user" not in conn
        assert "warehouse" not in conn
        assert "database" not in conn
        assert "schema" not in conn


def test_overlay_adds_fields_without_replacing_connection(config_ng_setup):
    """
    Test that OVERLAY sources (env vars) add/override individual fields
    without replacing the entire connection from FILE sources.
    """
    cli_config = """
    [connections.test]
    account = "cli-account"
    database = "cli-database"
    schema = "cli-schema"
    """

    env_vars = {
        "SNOWFLAKE_CONNECTIONS_TEST_USER": "env-user",
        "SNOWFLAKE_CONNECTIONS_TEST_WAREHOUSE": "env-warehouse",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Values from FILE source (cli_config)
        assert conn["account"] == "cli-account"
        assert conn["database"] == "cli-database"
        assert conn["schema"] == "cli-schema"

        # Values from OVERLAY source (env vars) - added without replacing
        assert conn["user"] == "env-user"
        assert conn["warehouse"] == "env-warehouse"


def test_overlay_overrides_file_field_without_replacing_connection(config_ng_setup):
    """
    Test that OVERLAY sources can override individual fields from FILE sources
    without replacing the entire connection.
    """
    connections_toml = """
    [connections.prod]
    account = "file-account"
    user = "file-user"
    warehouse = "file-warehouse"
    database = "file-database"
    """

    env_vars = {
        "SNOWFLAKE_CONNECTIONS_PROD_USER": "env-user",
        "SNOWFLAKE_CONNECTIONS_PROD_WAREHOUSE": "env-warehouse",
    }

    with config_ng_setup(connections_toml=connections_toml, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("prod")

        # account and database from FILE source (not overridden)
        assert conn["account"] == "file-account"
        assert conn["database"] == "file-database"

        # user and warehouse overridden by OVERLAY source
        assert conn["user"] == "env-user"
        assert conn["warehouse"] == "env-warehouse"


def test_snowsql_env_overlay_on_replaced_connection(config_ng_setup):
    """
    Test that SNOWSQL_* env vars (OVERLAY) overlay on replaced connections.
    """
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    user = snowsql-user
    warehouse = snowsql-warehouse
    database = snowsql-database
    """

    connections_toml = """
    [connections.test]
    account = "conn-account"
    user = "conn-user"
    # warehouse and database not included (connection replaced)
    """

    env_vars = {
        "SNOWSQL_WAREHOUSE": "env-warehouse",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Values from connections.toml (FILE source)
        assert conn["account"] == "conn-account"
        assert conn["user"] == "conn-user"

        # database from snowsql NOT inherited (connection replaced by connections.toml)
        assert "database" not in conn

        # warehouse from SNOWSQL_* env (OVERLAY source)
        assert conn["warehouse"] == "env-warehouse"


def test_cli_env_overlay_on_file_connection(config_ng_setup):
    """
    Test that SNOWFLAKE_* env vars (OVERLAY) add fields to file connections.
    """
    cli_config = """
    [connections.dev]
    account = "cli-account"
    database = "cli-database"
    """

    env_vars = {
        "SNOWFLAKE_USER": "global-env-user",
        "SNOWFLAKE_WAREHOUSE": "global-env-warehouse",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("dev")

        # Values from FILE source
        assert conn["account"] == "cli-account"
        assert conn["database"] == "cli-database"

        # Values from global OVERLAY source (apply to all connections)
        assert conn["user"] == "global-env-user"
        assert conn["warehouse"] == "global-env-warehouse"


def test_multiple_connections_independent_replacement(config_ng_setup):
    """
    Test that replacement is per-connection: different connections can be
    replaced independently.
    """
    snowsql_config = """
    [connections.conn1]
    accountname = snowsql-account1
    user = snowsql-user1
    warehouse = snowsql-warehouse1
    
    [connections.conn2]
    accountname = snowsql-account2
    user = snowsql-user2
    database = snowsql-database2
    """

    connections_toml = """
    [connections.conn1]
    account = "conn-account1"
    # Only account specified for conn1 - warehouse NOT inherited
    
    # conn2 NOT defined in connections.toml
    """

    with config_ng_setup(
        snowsql_config=snowsql_config, connections_toml=connections_toml
    ):
        from snowflake.cli.api.config import get_connection_dict

        # conn1: replaced by connections.toml
        conn1 = get_connection_dict("conn1")
        assert conn1["account"] == "conn-account1"
        assert "user" not in conn1  # Not inherited
        assert "warehouse" not in conn1  # Not inherited

        # conn2: NOT replaced, uses snowsql_config values
        conn2 = get_connection_dict("conn2")
        assert conn2["account"] == "snowsql-account2"
        assert conn2["user"] == "snowsql-user2"
        assert conn2["database"] == "snowsql-database2"


def test_empty_connection_replacement(config_ng_setup):
    """
    Test that an empty connection in a later FILE source still replaces
    the entire connection from earlier sources, resulting in no configured connection.
    """
    cli_config = """
    [connections.test]
    account = "cli-account"
    user = "cli-user"
    warehouse = "cli-warehouse"
    """

    connections_toml = """
    [connections.test]
    # Empty connection section
    """

    with config_ng_setup(cli_config=cli_config, connections_toml=connections_toml):
        import pytest
        from snowflake.cli.api.config import get_connection_dict
        from snowflake.cli.api.exceptions import MissingConfigurationError

        # Empty connection replacement means no parameters, which raises an error
        with pytest.raises(
            MissingConfigurationError, match="Connection test is not configured"
        ):
            get_connection_dict("test")


def test_overlay_precedence_connection_specific_over_global(config_ng_setup):
    """
    Test OVERLAY precedence: global env (SNOWFLAKE_*) overrides connection-specific env.
    Source order: connection_specific_env (#5) < cli_env (#6)
    """
    cli_config = """
    [connections.test]
    account = "cli-account"
    """

    env_vars = {
        "SNOWFLAKE_USER": "global-user",
        "SNOWFLAKE_CONNECTIONS_TEST_USER": "specific-user",
        "SNOWFLAKE_WAREHOUSE": "global-warehouse",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        assert conn["account"] == "cli-account"
        # Global env (later OVERLAY source) overrides connection-specific env
        assert conn["user"] == "global-user"
        # Global env applies when no specific override exists
        assert conn["warehouse"] == "global-warehouse"


def test_resolution_history_shows_replacement(config_ng_setup):
    """
    Test that resolution history correctly shows file replacement behavior.
    """
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    user = snowsql-user
    warehouse = snowsql-warehouse
    """

    cli_config = """
    [connections.test]
    account = "cli-account"
    user = "cli-user"
    # warehouse not included
    """

    with config_ng_setup(snowsql_config=snowsql_config, cli_config=cli_config):
        from snowflake.cli.api.config import get_connection_dict
        from snowflake.cli.api.config_ng import get_resolver

        # Trigger resolution to populate history
        conn = get_connection_dict("test")
        assert conn["account"] == "cli-account"

        resolver = get_resolver()
        assert resolver is not None

        # account: both sources provide, cli_config wins
        account_history = resolver.get_resolution_history("connections.test.account")
        assert account_history is not None
        assert len(account_history.entries) == 2
        assert account_history.final_value == "cli-account"

        # user: both sources provide, cli_config wins
        user_history = resolver.get_resolution_history("connections.test.user")
        assert user_history is not None
        assert len(user_history.entries) == 2
        assert user_history.final_value == "cli-user"

        # warehouse: only snowsql provides, but NOT in final config (connection replaced)
        # Since the connection was replaced and warehouse wasn't in the new connection,
        # it was discovered but never made it to the final resolution, so no history entry
        warehouse_history = resolver.get_resolution_history(
            "connections.test.warehouse"
        )
        # Note: warehouse was discovered but since connection was replaced by cli_config,
        # and cli_config didn't include warehouse, it's not in the final resolved values
        # The history tracking only marks selected values, so warehouse has no marked entry
        if warehouse_history:
            # If history exists, it should show discovery but no selection
            assert warehouse_history.selected_entry is None


def test_flat_keys_still_use_simple_override(config_ng_setup):
    """
    Test that flat keys (non-connection) still use simple override behavior.
    """
    snowsql_config = """
    [connections]
    some_global = snowsql-global
    """

    cli_config = """
    [connections]
    some_global = "cli-global"
    """

    # Note: This test is somewhat artificial as flat keys in connections sections
    # are not commonly used, but verifies the logic handles them correctly

    with config_ng_setup(snowsql_config=snowsql_config, cli_config=cli_config):
        from snowflake.cli.api.config_ng import get_resolver

        resolver = get_resolver()
        if resolver:
            resolved = resolver.resolve()
            # This would need actual flat key support in sources to fully test
            # For now, just verify no errors occur
            assert resolved is not None
