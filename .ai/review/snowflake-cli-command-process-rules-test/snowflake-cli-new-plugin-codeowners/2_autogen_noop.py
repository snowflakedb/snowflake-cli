from snowflake.cli.api.plugins.command import (
    SNOWCLI_ROOT_COMMAND_PATH,
    CommandSpec,
    CommandType,
    plugin_hook_impl,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin


class GitManager(SqlExecutionMixin):
    def show_repositories(self):
        return self.execute_query("SHOW GIT REPOSITORIES")

    def fetch_repo(self, repo_name: str):
        rows = self.show_repositories()
        for row in rows:
            if row["name"] == repo_name:
                return row
        return None


@plugin_hook_impl
def command_spec():
    from snowflake.cli._plugins.git import commands

    return CommandSpec(
        parent_command_path=SNOWCLI_ROOT_COMMAND_PATH,
        command_type=CommandType.COMMAND_GROUP,
        typer_instance=commands.app.create_instance(),
    )


def get_builtin_plugin_name_to_plugin_spec():
    from snowflake.cli._plugins.git import plugin_spec as git_plugin_spec
    from snowflake.cli._plugins.sql import plugin_spec as sql_plugin_spec

    return {
        "git": git_plugin_spec,
        "sql": sql_plugin_spec,
    }
