from __future__ import annotations

from pathlib import Path
from typing import Optional, Union

from snowcli.cli.common.sql_execution import SqlExecutionMixin


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

    def get(self, stage_name: str, dest_path: Path):
        stage_name = self.get_standard_stage_name(stage_name)
        return self._execute_query(f"get {stage_name} file://{dest_path}/")

    def put(
        self,
        local_path: Union[str, Path],
        stage_name: str,
        parallel: int = 4,
        overwrite: bool = False,
    ):
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

    def create(self, stage_name: str, comment: Optional[str] = None):
        query = f"create stage if not exists {stage_name}"
        if comment:
            query += f" comment='{comment}'"
        return self._execute_query(query)

    def drop(self, stage_name: str):
        return self._execute_query(f"drop stage {stage_name}")
