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
Simplified tests for config_ng using minimal mocking.

This test file demonstrates a simpler approach to testing configuration
resolution by:
1. Setting up temporary SNOWFLAKE_HOME with config files
2. Setting environment variables directly
3. Calling get_connection_dict() to test the actual public API
4. Minimal mocking - only using the real config_ng system

Uses the config_ng_setup fixture from conftest.py.
"""


# Tests for all 7 precedence levels


def test_level1_snowsql_config(config_ng_setup):
    """Base level: SnowSQL config provides values"""
    snowsql_config = """
    [connections.test]
    accountname = from-snowsql
    user = test-user
    password = test-password
    """

    with config_ng_setup(snowsql_config=snowsql_config):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-snowsql"
        assert conn["user"] == "test-user"
        assert conn["password"] == "test-password"


def test_level2_cli_config_overrides_snowsql(config_ng_setup):
    """CLI config.toml overrides SnowSQL config"""
    snowsql_config = """
    [connections.test]
    accountname = from-snowsql
    user = snowsql-user
    """

    cli_config = """
    [connections.test]
    account = "from-cli-config"
    user = "cli-config-user"
    """

    with config_ng_setup(snowsql_config=snowsql_config, cli_config=cli_config):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-cli-config"
        assert conn["user"] == "cli-config-user"


def test_level3_connections_toml_overrides_cli_config(config_ng_setup):
    """connections.toml overrides cli config.toml"""
    cli_config = """
    [connections.test]
    account = "from-cli-config"
    warehouse = "cli-warehouse"
    """

    connections_toml = """
    [connections.test]
    account = "from-connections-toml"
    warehouse = "connections-warehouse"
    """

    with config_ng_setup(cli_config=cli_config, connections_toml=connections_toml):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-connections-toml"
        assert conn["warehouse"] == "connections-warehouse"


def test_level4_snowsql_env_overrides_connections_toml(config_ng_setup):
    """SNOWSQL_* env vars override connections.toml"""
    connections_toml = """
    [connections.test]
    account = "from-connections-toml"
    database = "connections-db"
    """

    env_vars = {"SNOWSQL_ACCOUNT": "from-snowsql-env", "SNOWSQL_DATABASE": "env-db"}

    with config_ng_setup(connections_toml=connections_toml, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-snowsql-env"
        assert conn["database"] == "env-db"


def test_level5_connection_specific_env_overrides_snowsql_env(config_ng_setup):
    """SNOWFLAKE_CONNECTIONS_* overrides SNOWSQL_*"""
    env_vars = {
        "SNOWSQL_ACCOUNT": "from-snowsql-env",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "from-conn-specific-env",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-specific-role",
    }

    with config_ng_setup(env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-conn-specific-env"
        assert conn["role"] == "conn-specific-role"


def test_level6_general_env_overrides_connection_specific(config_ng_setup):
    """SNOWFLAKE_* overrides SNOWFLAKE_CONNECTIONS_*"""
    env_vars = {
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "from-conn-specific",
        "SNOWFLAKE_ACCOUNT": "from-general-env",
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")
        assert conn["account"] == "from-general-env"
        assert conn["schema"] == "general-schema"


def test_complete_7_level_chain(config_ng_setup):
    """All 7 levels with different keys showing complete precedence"""
    snowsql_config = """
    [connections.test]
    accountname = level1
    user = level1-user
    """

    cli_config = """
    [connections.test]
    account = "level2"
    password = "level2-pass"
    """

    connections_toml = """
    [connections.test]
    account = "level3"
    warehouse = "level3-wh"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "level4",
        "SNOWSQL_DATABASE": "level4-db",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "level5",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "level5-role",
        "SNOWFLAKE_ACCOUNT": "level6",
        "SNOWFLAKE_SCHEMA": "level6-schema",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Level 6 should win for account (general env)
        assert conn["account"] == "level6"

        # Level 6 provides schema (only level with it)
        assert conn["schema"] == "level6-schema"

        # Level 5 provides role (highest level with it)
        assert conn["role"] == "level5-role"

        # Level 4 provides database
        assert conn["database"] == "level4-db"

        # Level 3 provides warehouse
        assert conn["warehouse"] == "level3-wh"

        # Level 2 provides password
        assert conn["password"] == "level2-pass"

        # Level 1 provides user
        assert conn["user"] == "level1-user"


def test_get_connection_dict_uses_config_ng_when_enabled(config_ng_setup):
    """Validate that get_connection_dict delegates to config_ng when flag is set"""

    cli_config = """
    [connections.test]
    account = "test-account"
    user = "test-user"
    """

    with config_ng_setup(cli_config=cli_config):
        from snowflake.cli.api.config import get_connection_dict
        from snowflake.cli.api.config_provider import (
            AlternativeConfigProvider,
            get_config_provider_singleton,
        )

        # Verify we're using AlternativeConfigProvider
        provider = get_config_provider_singleton()
        assert isinstance(provider, AlternativeConfigProvider)

        # Verify resolution works
        conn = get_connection_dict("test")

        assert conn["account"] == "test-account"
        assert conn["user"] == "test-user"


def test_precedence_with_multiple_connections(config_ng_setup):
    """Test that precedence works correctly for multiple connections"""
    cli_config = """
    [connections.conn1]
    account = "conn1-account"
    user = "conn1-user"
    
    [connections.conn2]
    account = "conn2-account"
    user = "conn2-user"
    """

    env_vars = {
        "SNOWFLAKE_CONNECTIONS_CONN1_ACCOUNT": "conn1-env",
        "SNOWFLAKE_SCHEMA": "common-schema",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        # conn1 should have env override
        conn1 = get_connection_dict("conn1")
        assert conn1["account"] == "conn1-env"  # From connection-specific env
        assert conn1["user"] == "conn1-user"  # From config file
        assert conn1["schema"] == "common-schema"  # From general env

        # conn2 should use config values
        conn2 = get_connection_dict("conn2")
        assert conn2["account"] == "conn2-account"  # From config file
        assert conn2["user"] == "conn2-user"  # From config file
        assert conn2["schema"] == "common-schema"  # From general env


def test_snowsql_key_mapping(config_ng_setup):
    """Test that SnowSQL key names are properly mapped to CLI names"""
    snowsql_config = """
    [connections.test]
    accountname = test-account
    username = test-user
    dbname = test-db
    schemaname = test-schema
    rolename = test-role
    warehousename = test-warehouse
    pwd = test-password
    """

    with config_ng_setup(snowsql_config=snowsql_config):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # All SnowSQL names should be mapped to CLI names
        assert conn["account"] == "test-account"
        assert conn["user"] == "test-user"
        assert conn["database"] == "test-db"
        assert conn["schema"] == "test-schema"
        assert conn["role"] == "test-role"
        assert conn["warehouse"] == "test-warehouse"
        assert conn["password"] == "test-password"


def test_empty_config_files(config_ng_setup):
    """Test behavior with empty/missing config files"""
    # Only set env vars, no config files
    env_vars = {
        "SNOWFLAKE_ACCOUNT": "env-only-account",
        "SNOWFLAKE_USER": "env-only-user",
    }

    with config_ng_setup(env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("default")
        assert conn["account"] == "env-only-account"
        assert conn["user"] == "env-only-user"


# Group 1: Non-Adjacent 2-Source Tests


def test_snowsql_config_with_snowsql_env_direct(config_ng_setup):
    """Test SnowSQL env overrides SnowSQL config when intermediate sources absent"""
    snowsql_config = """
    [connections.test]
    accountname = from-config
    user = config-user
    database = config-db
    """

    env_vars = {"SNOWSQL_ACCOUNT": "from-env", "SNOWSQL_DATABASE": "env-db"}

    with config_ng_setup(snowsql_config=snowsql_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Level 4 (SnowSQL env) wins for account and database
        # Level 1 (SnowSQL config) wins for user
        expected = {
            "account": "from-env",
            "user": "config-user",
            "database": "env-db",
        }
        assert conn == expected


def test_snowsql_config_with_general_env_direct(config_ng_setup):
    """Test general env overrides SnowSQL config across all intermediate levels"""
    snowsql_config = """
    [connections.test]
    accountname = from-config
    user = config-user
    warehouse = config-warehouse
    """

    env_vars = {"SNOWFLAKE_ACCOUNT": "from-env", "SNOWFLAKE_WAREHOUSE": "env-warehouse"}

    with config_ng_setup(snowsql_config=snowsql_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "from-env",
            "user": "config-user",
            "warehouse": "env-warehouse",
        }
        assert conn == expected


def test_cli_config_with_general_env_direct(config_ng_setup):
    """Test general env overrides CLI config when intermediate sources absent"""
    cli_config = """
    [connections.test]
    account = "from-cli"
    user = "cli-user"
    role = "cli-role"
    """

    env_vars = {"SNOWFLAKE_ACCOUNT": "from-env", "SNOWFLAKE_ROLE": "env-role"}

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "from-env",
            "user": "cli-user",
            "role": "env-role",
        }
        assert conn == expected


def test_connections_toml_with_general_env_direct(config_ng_setup):
    """Test general env overrides Connections TOML directly"""
    connections_toml = """
    [connections.test]
    account = "from-toml"
    user = "toml-user"
    schema = "toml-schema"
    """

    env_vars = {"SNOWFLAKE_ACCOUNT": "from-env", "SNOWFLAKE_SCHEMA": "env-schema"}

    with config_ng_setup(connections_toml=connections_toml, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "from-env",
            "user": "toml-user",
            "schema": "env-schema",
        }
        assert conn == expected


# Group 2: Strategic 3-Source Tests


def test_all_file_sources_precedence(config_ng_setup):
    """Test precedence among all three file-based sources"""
    snowsql_config = """
    [connections.test]
    accountname = from-snowsql
    user = snowsql-user
    warehouse = snowsql-warehouse
    password = snowsql-pass
    """

    cli_config = """
    [connections.test]
    account = "from-cli"
    user = "cli-user"
    password = "cli-pass"
    """

    connections_toml = """
    [connections.test]
    account = "from-connections"
    password = "connections-pass"
    """

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "from-connections",  # Level 3 wins
            "user": "cli-user",  # Level 2 wins
            "warehouse": "snowsql-warehouse",  # Level 1 only source
            "password": "connections-pass",  # Level 3 wins
        }
        assert conn == expected


def test_all_env_sources_precedence(config_ng_setup):
    """Test precedence among all three environment variable types"""
    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWSQL_DATABASE": "snowsql-db",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-role",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "schema": "general-schema",  # Level 6 only source
            "role": "conn-role",  # Level 5 only source
            "database": "snowsql-db",  # Level 4 only source
        }
        assert conn == expected


def test_file_and_env_mix_with_gaps(config_ng_setup):
    """Test precedence with gaps in source chain"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    user = snowsql-user
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    warehouse = "toml-warehouse"
    """

    env_vars = {"SNOWFLAKE_ACCOUNT": "env-account"}

    with config_ng_setup(
        snowsql_config=snowsql_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "env-account",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only source
            "warehouse": "toml-warehouse",  # Level 3 only source
        }
        assert conn == expected


def test_cli_config_with_two_env_types(config_ng_setup):
    """Test CLI config as base with two env override types"""
    cli_config = """
    [connections.test]
    account = "cli-account"
    user = "cli-user"
    database = "cli-db"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_DATABASE": "conn-db",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "conn-specific",  # Level 5 wins
            "user": "cli-user",  # Level 2 only source
            "database": "conn-db",  # Level 5 wins
        }
        assert conn == expected


# Group 3: 4-Source Combinations


def test_all_files_plus_snowsql_env(config_ng_setup):
    """Test all file sources with SnowSQL environment override"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    """

    cli_config = """
    [connections.test]
    account = "cli-config"
    warehouse = "cli-warehouse"
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    database = "toml-db"
    """

    env_vars = {"SNOWSQL_ACCOUNT": "env-account"}

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "env-account",  # Level 4 wins
            "user": "snowsql-user",  # Level 1 only
            "warehouse": "cli-warehouse",  # Level 2 only
            "database": "toml-db",  # Level 3 only
        }
        assert conn == expected


def test_all_files_plus_general_env(config_ng_setup):
    """Test all file sources with general environment override"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    """

    cli_config = """
    [connections.test]
    account = "cli-config"
    role = "cli-role"
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    warehouse = "toml-warehouse"
    """

    env_vars = {
        "SNOWFLAKE_ACCOUNT": "env-account",
        "SNOWFLAKE_WAREHOUSE": "env-warehouse",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "env-account",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only
            "role": "cli-role",  # Level 2 only
            "warehouse": "env-warehouse",  # Level 6 wins
        }
        assert conn == expected


def test_cli_config_with_all_env_types(config_ng_setup):
    """Test single file source with all three environment types"""
    cli_config = """
    [connections.test]
    account = "cli-account"
    user = "cli-user"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWSQL_DATABASE": "snowsql-db",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-role",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "user": "cli-user",  # Level 2 only
            "database": "snowsql-db",  # Level 4 only
            "role": "conn-role",  # Level 5 only
            "schema": "general-schema",  # Level 6 only
        }
        assert conn == expected


def test_two_files_two_envs_with_gap(config_ng_setup):
    """Test non-adjacent file sources with non-adjacent env sources"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    warehouse = "toml-warehouse"
    """

    env_vars = {
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_DATABASE": "conn-db",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only
            "warehouse": "toml-warehouse",  # Level 3 only
            "database": "conn-db",  # Level 5 only
            "schema": "general-schema",  # Level 6 only
        }
        assert conn == expected


# Group 4: 5-Source Combinations


def test_all_files_plus_two_env_types(config_ng_setup):
    """Test all file sources with two environment override types"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    """

    cli_config = """
    [connections.test]
    account = "cli-config"
    password = "cli-password"
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    warehouse = "toml-warehouse"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_WAREHOUSE": "conn-warehouse",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "conn-specific",  # Level 5 wins
            "user": "snowsql-user",  # Level 1 only
            "password": "cli-password",  # Level 2 only
            "warehouse": "conn-warehouse",  # Level 5 wins
        }
        assert conn == expected


def test_two_files_all_envs(config_ng_setup):
    """Test two file sources with all three environment types"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    """

    cli_config = """
    [connections.test]
    account = "cli-config"
    password = "cli-password"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWSQL_DATABASE": "snowsql-db",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "conn-specific",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-role",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_WAREHOUSE": "general-warehouse",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only
            "password": "cli-password",  # Level 2 only
            "database": "snowsql-db",  # Level 4 only
            "role": "conn-role",  # Level 5 only
            "warehouse": "general-warehouse",  # Level 6 only
        }
        assert conn == expected


def test_connections_toml_with_all_env_types(config_ng_setup):
    """Test Connections TOML with all environment override types"""
    connections_toml = """
    [connections.test]
    account = "toml-account"
    user = "toml-user"
    warehouse = "toml-warehouse"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWSQL_DATABASE": "snowsql-db",
        "SNOWFLAKE_CONNECTIONS_TEST_WAREHOUSE": "conn-warehouse",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-role",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(connections_toml=connections_toml, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "user": "toml-user",  # Level 3 only
            "warehouse": "conn-warehouse",  # Level 5 wins
            "database": "snowsql-db",  # Level 4 only
            "role": "conn-role",  # Level 5 only
            "schema": "general-schema",  # Level 6 only
        }
        assert conn == expected


def test_snowsql_and_connections_with_all_envs(config_ng_setup):
    """Test two non-adjacent file sources with all environment types"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-config
    user = snowsql-user
    password = snowsql-password
    """

    connections_toml = """
    [connections.test]
    account = "toml-account"
    warehouse = "toml-warehouse"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "snowsql-env",
        "SNOWSQL_WAREHOUSE": "snowsql-warehouse",
        "SNOWFLAKE_CONNECTIONS_TEST_PASSWORD": "conn-password",
        "SNOWFLAKE_CONNECTIONS_TEST_ROLE": "conn-role",
        "SNOWFLAKE_ACCOUNT": "general-env",
        "SNOWFLAKE_DATABASE": "general-db",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "general-env",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only
            "password": "conn-password",  # Level 5 wins
            "warehouse": "snowsql-warehouse",  # Level 4 wins (overrides level 3)
            "role": "conn-role",  # Level 5 only
            "database": "general-db",  # Level 6 only
        }
        assert conn == expected


# Group 5: Edge Cases


def test_multiple_connections_different_source_patterns(config_ng_setup):
    """Test that different connections can have different active sources"""
    cli_config = """
    [connections.conn1]
    account = "conn1-cli"
    user = "conn1-user"
    
    [connections.conn2]
    account = "conn2-cli"
    user = "conn2-user"
    """

    connections_toml = """
    [connections.conn1]
    warehouse = "conn1-warehouse"
    
    [connections.conn3]
    account = "conn3-toml"
    user = "conn3-user"
    """

    env_vars = {
        "SNOWFLAKE_CONNECTIONS_CONN1_ACCOUNT": "conn1-env",
        "SNOWFLAKE_CONNECTIONS_CONN2_DATABASE": "conn2-db",
        "SNOWFLAKE_SCHEMA": "common-schema",
    }

    with config_ng_setup(
        cli_config=cli_config, connections_toml=connections_toml, env_vars=env_vars
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn1 = get_connection_dict("conn1")
        expected1 = {
            "account": "conn1-env",  # Connection-specific env wins
            "user": "conn1-user",  # CLI config
            "warehouse": "conn1-warehouse",  # Connections TOML
            "schema": "common-schema",  # General env
        }
        assert conn1 == expected1

        conn2 = get_connection_dict("conn2")
        expected2 = {
            "account": "conn2-cli",  # CLI config
            "user": "conn2-user",  # CLI config
            "database": "conn2-db",  # Connection-specific env
            "schema": "common-schema",  # General env
        }
        assert conn2 == expected2

        conn3 = get_connection_dict("conn3")
        expected3 = {
            "account": "conn3-toml",  # Connections TOML
            "user": "conn3-user",  # Connections TOML
            "schema": "common-schema",  # General env
        }
        assert conn3 == expected3


def test_snowsql_key_mapping_with_precedence(config_ng_setup):
    """Test SnowSQL legacy key names work correctly across precedence levels"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    username = snowsql-user
    dbname = snowsql-db
    schemaname = snowsql-schema
    rolename = snowsql-role
    warehousename = snowsql-warehouse
    """

    cli_config = """
    [connections.test]
    account = "cli-account"
    database = "cli-db"
    """

    env_vars = {
        "SNOWFLAKE_ACCOUNT": "env-account",
        "SNOWFLAKE_SCHEMA": "env-schema",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "env-account",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only (mapped from username)
            "database": "cli-db",  # Level 2 wins
            "schema": "env-schema",  # Level 6 wins
            "role": "snowsql-role",  # Level 1 only (mapped from rolename)
            "warehouse": "snowsql-warehouse",  # Level 1 only (mapped from warehousename)
        }
        assert conn == expected


def test_empty_intermediate_sources_dont_break_chain(config_ng_setup):
    """Test that empty config files don't prevent higher sources from working"""
    snowsql_config = """
    [connections.test]
    accountname = snowsql-account
    user = snowsql-user
    """

    # Empty CLI config and connections.toml
    cli_config = ""
    connections_toml = ""

    env_vars = {"SNOWFLAKE_ACCOUNT": "env-account"}

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        expected = {
            "account": "env-account",  # Level 6 wins
            "user": "snowsql-user",  # Level 1 only
        }
        assert conn == expected


def test_account_parameter_across_all_sources(config_ng_setup):
    """Test account parameter defined in all sources follows precedence"""
    snowsql_config = """
    [connections.test]
    accountname = level1-account
    """

    cli_config = """
    [connections.test]
    account = "level2-account"
    """

    connections_toml = """
    [connections.test]
    account = "level3-account"
    """

    env_vars = {
        "SNOWSQL_ACCOUNT": "level4-account",
        "SNOWFLAKE_CONNECTIONS_TEST_ACCOUNT": "level5-account",
        "SNOWFLAKE_ACCOUNT": "level6-account",
    }

    with config_ng_setup(
        snowsql_config=snowsql_config,
        cli_config=cli_config,
        connections_toml=connections_toml,
        env_vars=env_vars,
    ):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("test")

        # Only account should be present since all sources only provide account
        expected = {
            "account": "level6-account",  # Level 6 (general env) wins
        }
        assert conn == expected
