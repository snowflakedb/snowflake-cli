import pytest


@pytest.fixture
def enable_snowgit_config(test_snowcli_config, snowflake_home):
    config = snowflake_home / "config_with_snowgit_enabled.toml"
    config.write_text(
        f"""
{test_snowcli_config.read_text()}

[cli.features]
enable_snowgit = true
"""
    )
    yield config
