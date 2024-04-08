from textwrap import dedent

import pytest


def test_override_build_in_commands(runner, test_root_path, _install_plugin, caplog):
    config_path = (
        test_root_path / "test_data" / "configs" / "override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [override]: Cannot add command [snow connection list] because it already exists."
    )
    assert result.output == dedent(
        """\
     Outside command code
     +----------------------------------------------------+
     | connection_name | parameters          | is_default |
     |-----------------+---------------------+------------|
     | test            | {'account': 'test'} | False      |
     +----------------------------------------------------+
    """
    )


def test_disabled_plugin_is_not_executed(
    runner, test_root_path, _install_plugin, caplog
):
    config_path = (
        test_root_path
        / "test_data"
        / "configs"
        / "disabled_override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert len(caplog.messages) == 0
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

    path = test_root_path / ".." / "test_external_plugins" / "override_build_in_command"
    subprocess.check_call(["pip", "install", path])
