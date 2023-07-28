from __future__ import annotations

from pathlib import Path

import typer

from snowcli.cli.common.flags import DEFAULT_CONTEXT_SETTINGS, ConnectionOption
from snowcli.output.decorators import with_output
from snowcli.snow_connector import SqlExecutionMixin

app = typer.Typer(
    name="stage",
    context_settings=DEFAULT_CONTEXT_SETTINGS,
    help="Manage stages",
)

StageNameOption = typer.Argument(..., help="Stage name.")


class StageManager(SqlExecutionMixin):
    @staticmethod
    def get_standard_stage_name(name: str) -> str:
        # Handle embedded stages
        if name.startswith("snow://"):
            return name

        return f"@{name}"

    def list(self, stage_name: str):
        stage_name = self.get_standard_stage_name(stage_name)
        return self._execute_query(f"ls {stage_name}")

    def get(self, stage_name: str, dest_path: str):
        stage_name = self.get_standard_stage_name(stage_name)
        return self._execute_query(f"get {stage_name} file://{dest_path}/")

    def put(self, local_path: str, stage_name: str, parallel: int, overwrite: bool):
        stage_name = self.get_standard_stage_name(stage_name)
        return self._execute_query(
            f"put file://{local_path} {stage_name} "
            f"auto_compress=false parallel={parallel} overwrite={overwrite}"
        )

    def remove(self, stage_name: str, path: str):
        stage_name = self.get_standard_stage_name(stage_name)
        path = path if path.startswith("/") else "/" + path
        return self._execute_query(f"remove {stage_name}{path}")

    def show(self):
        return self._execute_query("show stages")

    def create(self, stage_name: str):
        return self._execute_query(f"create stage if not exists {stage_name}")

    def drop(self, stage_name: str):
        return self._execute_query(f"drop stage {stage_name}")


@app.command("list")
@with_output
def stage_list(
    connection_name: str = ConnectionOption,
    stage_name=typer.Argument(None, help="Name of stage"),
):
    """
    List stage contents or shows available stages if stage name not provided.
    """
    manager = StageManager.from_connection(connection_name=connection_name)

    if stage_name:
        return manager.list(stage_name=stage_name)
    return manager.show()


@app.command("get")
@with_output
def stage_get(
    connection_name: str = ConnectionOption,
    stage_name: str = StageNameOption,
    path: Path = typer.Argument(
        Path.cwd(),
        exists=False,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="Directory location to store downloaded files",
    ),
):
    """
    Download all files from a stage to a local directory.
    """
    return StageManager.from_connection(connection_name=connection_name).get(
        stage_name=stage_name, dest_path=path
    )


@app.command("put")
@with_output
def stage_put(
    connection_name: str = ConnectionOption,
    path: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=True,
        writable=True,
        resolve_path=True,
        help="File or directory to upload to stage",
    ),
    name: str = StageNameOption,
    overwrite: bool = typer.Option(
        False,
        help="Overwrite existing files in stage",
    ),
    parallel: int = typer.Option(
        4,
        help="Number of parallel threads to use for upload",
    ),
):
    """
    Upload files to a stage from a local client
    """
    manager = StageManager.from_connection(connection_name=connection_name)
    local_path = str(path) + "/*" if path.is_dir() else str(path)

    return manager.put(
        local_path=local_path, stage_name=name, overwrite=overwrite, parallel=parallel
    )


@app.command("create")
@with_output
def stage_create(
    connection_name: str = ConnectionOption,
    name: str = StageNameOption,
):
    """
    Create stage if not exists.
    """
    return StageManager.from_connection(connection_name=connection_name).create(
        stage_name=name
    )


@app.command("drop")
@with_output
def stage_drop(
    connection_name: str = ConnectionOption,
    name: str = StageNameOption,
):
    """
    Drop stage
    """
    return StageManager.from_connection(connection_name=connection_name).drop(
        stage_name=name
    )


@app.command("remove")
@with_output
def stage_remove(
    connection_name: str = ConnectionOption,
    stage_name: str = StageNameOption,
    file_name: str = typer.Argument(..., help="File name"),
):
    """
    Remove file from stage
    """

    return StageManager.from_connection(connection_name=connection_name).remove(
        stage_name=stage_name, path=file_name
    )
