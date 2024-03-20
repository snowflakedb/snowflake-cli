from textwrap import dedent

from tests.plugin.fixtures import install_plugin  # noqa


def test_broken_command_path_plugin(runner, test_root_path, install_plugin, caplog):
    install_plugin("broken_plugin")
    config_path = test_root_path / "test_data" / "configs" / "broken_plugin_config.toml"

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [broken_plugin]: Invalid command path [snow broken run]. Command group [broken] does not exist."
    )
    assert result.output == dedent(
        """+---------------------------------------+
| connection_name | parameters          |
|-----------------+---------------------|
| test            | {'account': 'test'} |
+---------------------------------------+
    """
    )
