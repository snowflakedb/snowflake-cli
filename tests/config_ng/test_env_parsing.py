# Copyright (c) 2024 Snowflake Inc.

"""Focused tests for environment variable parsing in config_ng."""


def test_connection_specific_env_with_underscores(config_ng_setup):
    """Connection names containing underscores should parse correctly.

    Also validate keys that themselves contain underscores (e.g., PRIVATE_KEY_PATH).
    """

    env_vars = {
        # Connection-specific variables for connection name with underscores
        "SNOWFLAKE_CONNECTIONS_DEV_US_EAST_ACCOUNT": "from-specific",
        "SNOWFLAKE_CONNECTIONS_DEV_US_EAST_PRIVATE_KEY_PATH": "/tmp/example_key.pem",
        # General env remains available for other flat keys
        "SNOWFLAKE_SCHEMA": "general-schema",
    }

    with config_ng_setup(env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict

        conn = get_connection_dict("dev_us_east")

        assert conn["account"] == "from-specific"
        assert conn["private_key_path"] == "/tmp/example_key.pem"
        # Ensure general env still contributes flat keys
        assert conn["schema"] == "general-schema"
