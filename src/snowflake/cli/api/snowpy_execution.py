class SnowpyExecutionMixin:
    def __init__(self):
        self._root = None

    @property
    def root(self):
        if not self._root:
            from snowflake.cli.api.cli_global_context import get_cli_context
            from snowflake.core import Root

            connection = get_cli_context().connection
            self._root = Root(connection)
        return self._root
