from textwrap import dedent

import pytest


def test_broken_command_path_plugin(runner, test_root_path, _install_plugin, caplog):
    config_path = test_root_path / "test_data" / "configs" / "broken_plugin_config.toml"

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [broken_plugin]: Invalid command path [snow broken run]. Command group [broken] does not exist."
    )
    assert result.output == dedent(
        """\
     +----------------------------------------------------+
     | connection_name | parameters          | is_default |
     |-----------------+---------------------+------------|
     | test            | {'account': 'test'} | False      |
     +----------------------------------------------------+
    """
    )


@pytest.fixture(scope="module")
def _install_plugin(test_root_path):
    import subprocess

    path = test_root_path / ".." / "test_external_plugins" / "broken_plugin"
    subprocess.check_call(["pip", "install", path])
