from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager


def experimental_behaviour_enabled() -> bool:
    return snow_cli_global_context_manager.get_global_context_copy().experimental
