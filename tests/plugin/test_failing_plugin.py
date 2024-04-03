from textwrap import dedent

import pytest


def test_failing_plugin(runner, test_root_path, _install_plugin, caplog):
    config_path = (
        test_root_path / "test_data" / "configs" / "failing_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [failing_plugin]: Some error in plugin"
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

    path = test_root_path / ".." / "test_external_plugins" / "failing_plugin"
    subprocess.check_call(["pip", "install", path])
