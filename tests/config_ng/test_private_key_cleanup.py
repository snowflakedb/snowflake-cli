"""Tests for temporary private_key_raw file lifecycle and cleanup."""

from pathlib import Path


def test_private_key_raw_creates_and_cleans_temp_file(config_ng_setup, tmp_path):
    priv_key_content = (
        """-----BEGIN PRIVATE KEY-----\nABC\n-----END PRIVATE KEY-----\n"""
    )

    cli_config = """
    [connections.test]
    user = "cli-user"
    """

    env_vars = {
        # Provide private_key_raw via env to trigger transformation
        "SNOWFLAKE_CONNECTIONS_TEST_PRIVATE_KEY_RAW": priv_key_content,
    }

    with config_ng_setup(cli_config=cli_config, env_vars=env_vars):
        from snowflake.cli.api.config import get_connection_dict
        from snowflake.cli.api.config_provider import (
            get_config_provider_singleton,
            reset_config_provider,
        )

        provider = get_config_provider_singleton()

        conn = get_connection_dict("test")
        temp_path = Path(conn["private_key_file"])  # should exist now
        assert temp_path.exists()
        assert temp_path.read_text() == priv_key_content

        # Reset provider triggers cleanup
        reset_config_provider()

        # File should be gone after cleanup
        assert not temp_path.exists()
