from textwrap import dedent

from tests.plugin.fixtures import install_plugin  # noqa


def test_override_build_in_commands(runner, test_root_path, install_plugin, caplog):
    install_plugin("override_build_in_command")

    config_path = (
        test_root_path / "test_data" / "configs" / "override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [override]: Cannot add command [snow connection list] because it already exists."
    )
    assert result.output == dedent(
        """Outside command code
+---------------------------------------+
| connection_name | parameters          |
|-----------------+---------------------|
| test            | {'account': 'test'} |
+---------------------------------------+
    """
    )


def test_disabled_plugin_is_not_executed(
    runner, test_root_path, install_plugin, caplog
):
    install_plugin("override_build_in_command")
    config_path = (
        test_root_path
        / "test_data"
        / "configs"
        / "disabled_override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert len(caplog.messages) == 0
    assert result.output == dedent(
        """+---------------------------------------+
| connection_name | parameters          |
|-----------------+---------------------|
| test            | {'account': 'test'} |
+---------------------------------------+
    """
    )
