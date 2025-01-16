import functools

from snowflake.cli._plugins.notebook.notebook_entity_model import NotebookEntityModel
from snowflake.cli._plugins.workspace.context import ActionContext
from snowflake.cli.api.console.console import cli_console
from snowflake.cli.api.entities.common import EntityBase, get_sql_executor


class NotebookEntity(EntityBase[NotebookEntityModel]):
    """
    A notebook.
    """

    @functools.cached_property
    def _sql_executor(self):
        return get_sql_executor()

    def _upload_notebook_file_to_stage(self):
        cli_console.step("x")

    def _create_from_file(self):
        cli_console.step("d")

    def action_deploy(self, action_ctx: ActionContext, *args, **kwargs):
        notebook_id = self.model.fqn
        with cli_console.phase(f"Deploying notebook {notebook_id}"):
            self._upload_notebook_file_to_stage()
            self._create_from_file()
