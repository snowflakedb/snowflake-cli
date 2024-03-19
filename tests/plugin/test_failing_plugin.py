from textwrap import dedent

from tests.plugin.fixtures import install_plugin  # noqa


def test_failing_plugin(runner, test_root_path, install_plugin, caplog):
    install_plugin("failing_plugin")
    config_path = (
        test_root_path / "test_data" / "configs" / "failing_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [failing_plugin]: Some error in plugin"
    )
    assert result.output == dedent(
        """+---------------------------------------+
| connection_name | parameters          |
|-----------------+---------------------|
| test            | {'account': 'test'} |
+---------------------------------------+
    """
    )
