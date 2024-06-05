from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.plugins.workspace.manager import WorkspaceManager

app = SnowTyper(
    name="ws",
    help="Manages workspaces in Snowflake.",
)


@app.command(requires_connection=False)
def deploy(
    key: str,
    **options,
):
    """Deploys an entity."""

    # TODO Read from the actual definition file
    workspace_definition = {
        "stage": {
            "schema": "public",
            "name": "stage1",
        },
        "entities": {
            "ui": {
                "type": "streamlit",
                "name": "dashboard",
                "stage": "stage2",  # used when deployed as standalone
                "meta": {
                    "files": "src/ui/**/*",
                },
            },
            "pkg": {
                "type": "application package",
                "name": "mypkg",
                "children": [
                    {
                        "key": "ui",
                        "extends": "ui",
                    }
                ],
                "meta": {
                    "files": "src/app/**/*",
                },
            },
            "app": {"type": "application", "from": "pkg", "name": "myapp"},
        },
    }

    manager = WorkspaceManager(workspace_definition=workspace_definition)
    plan = manager.deploy(key)

    return MessageResult(plan)
