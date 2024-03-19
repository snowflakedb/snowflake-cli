import pytest


def test_override_build_in_commands(runner, test_root_path, _install_plugin, snapshot):
    config_path = (
        test_root_path / "test_data" / "configs" / "override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert result.output == snapshot


def test_disabled_plugin_is_not_executed(
    runner, test_root_path, _install_plugin, snapshot
):
    config_path = (
        test_root_path
        / "test_data"
        / "configs"
        / "disabled_override_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert result.output == snapshot


@pytest.fixture(scope="module")
def _install_plugin(test_root_path):
    import subprocess

    path = test_root_path / ".." / "test_external_plugins" / "override_build_in_command"
    subprocess.check_call(["pip", "install", path])
