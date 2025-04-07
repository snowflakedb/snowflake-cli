import pytest
from click import ClickException
from snowflake.cli._plugins.project.project_entity_model import MANIFEST_FILE_NAME
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.utils import get_entity_for_operation


def test_project_entity_raises_when_manifest_file_does_not_exist(project_directory):
    with project_directory("dcm_project") as project_root:
        (project_root / MANIFEST_FILE_NAME).unlink()
        with pytest.raises(ClickException) as err:
            cli_context = get_cli_context()
            get_entity_for_operation(
                cli_context=cli_context,
                entity_id="my_project",
                project_definition=cli_context.project_definition,
                entity_type="project",
            )
        assert f"{MANIFEST_FILE_NAME} was not found in project root directory" == str(
            err.value
        )
