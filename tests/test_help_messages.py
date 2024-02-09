from snowflake.cli.app.commands_registration.command_plugins_loader import (
    load_only_builtin_command_plugins,
)


def test_help_messages(runner, snapshot):
    """
    Iterate through all commands and check all their help messages against the snapshot
    """
    IGNORE_PLUGINS = ["render"]

    def _invoke_command(command):
        result = runner.invoke(command + ["--help"])
        assert result.exit_code == 0
        assert result.output == snapshot(name=".".join(command))

    def _invoke_all_commands(command, path):
        _invoke_command(path)
        for subpath, subcommand in getattr(command, "commands", {}).items():
            path.append(subpath)
            _invoke_all_commands(subcommand, path)
            path.pop()

    builtin_plugins = load_only_builtin_command_plugins()
    for plugin in builtin_plugins:
        spec = plugin.command_spec
        if not plugin.plugin_name in IGNORE_PLUGINS:
            _invoke_all_commands(spec.command, spec.full_command_path.path_segments)
