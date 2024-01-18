from snowflake.cli.api.cli_global_context import cli_context


def experimental_behaviour_enabled() -> bool:
    return cli_context.experimental
