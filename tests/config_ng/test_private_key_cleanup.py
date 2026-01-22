"""Tests for handling private_key_raw without persisting to disk."""


def test_private_key_raw_kept_in_memory(config_ng_setup):
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

        conn = get_connection_dict("test")
        assert "private_key_raw" in conn
        assert conn["private_key_raw"] == priv_key_content
        assert "private_key_file" not in conn
