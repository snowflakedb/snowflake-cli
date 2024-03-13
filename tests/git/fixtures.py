import pytest


@pytest.fixture(scope="module")
def enable_snowgit_config(test_snowcli_config):
    test_snowcli_config.write_text(
        f"""
{test_snowcli_config.read_text()}

[cli.features]
enable_snowgit = true
"""
    )
    yield test_snowcli_config
