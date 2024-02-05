def test_override_build_in_commands(runner, test_root_path, caplog):
    import subprocess

    path = test_root_path / ".." / "test_external_plugins" / "override_build_in_command"
    subprocess.check_call(["pip", "install", path])

    config_path = (
        test_root_path / "test_data" / "configs" / "override_plugin_config.toml"
    )

    runner.invoke(["--config-file", config_path, "--help"])

    assert (
        caplog.messages[0]
        == "Cannot register plugin [override]: Cannot add command [snow connection add] because it already exists."
    )
