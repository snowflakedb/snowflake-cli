import pytest


def test_failing_plugin(runner, test_root_path, _install_plugin, snapshot):
    config_path = (
        test_root_path / "test_data" / "configs" / "failing_plugin_config.toml"
    )

    result = runner.invoke(["--config-file", config_path, "connection", "list"])

    assert result.output == snapshot


@pytest.fixture(scope="module")
def _install_plugin(test_root_path):
    import subprocess

    path = test_root_path / ".." / "test_external_plugins" / "failing_plugin"
    subprocess.check_call(["pip", "install", path])
