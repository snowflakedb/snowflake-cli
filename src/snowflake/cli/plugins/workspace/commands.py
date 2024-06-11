from snowflake.cli.api.commands.snow_typer import SnowTyper
from snowflake.cli.api.output.types import MessageResult
from snowflake.cli.plugins.workspace.manager import WorkspaceManager

app = SnowTyper(
    name="ws",
    help="Manages workspaces in Snowflake.",
)


@app.command(requires_connection=True)
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
                "stage": "stage1",  # used when deployed as standalone
                "main_file": "src/ui/main.py",
                "meta": {
                    "files": [
                        {
                            "src": "src/ui/main.py",
                            "dest": "main.py",
                        },
                        {
                            "src": "src/ui/environment.yml",
                            "dest": "environment.yml",
                        },
                    ],
                },
                "depends_on": ["datalog"],
            },
            "datalog": {
                "type": "table",
                "name": "log",
                "columns": ["value TEXT NOT NULL"],
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
                    "files": [
                        {
                            "src": "src/app/README.md",
                            "dest": "README.md",
                        },
                        {
                            "src": "src/app/manifest.yml",
                            "dest": "manifest.yml",
                        },
                        {
                            "src": "src/app/setup_script.sql",
                            "dest": "setup_script.sql",
                        },
                    ],
                },
            },
            "app": {
                #
                "type": "application",
                "from": "pkg",
                "name": "myapp",
            },
        },
    }

    manager = WorkspaceManager(workspace_definition=workspace_definition)
    deploy_plan = manager.deploy(key)
    with open("output/deploy/deploy_plan.sql", "w") as deploy_plan_file:
        deploy_plan_file.write(deploy_plan.create_deploy_plan_sql())
    # manager.execute_plan(deploy_plan)

    return MessageResult(deploy_plan)
