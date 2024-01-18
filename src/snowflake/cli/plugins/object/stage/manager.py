from __future__ import annotations

import logging
import re
from contextlib import nullcontext
from pathlib import Path
from typing import Optional, Union

from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.utils.path_utils import path_resolver
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


UNQUOTED_FILE_URI_REGEX = r"[\w/*?\-.=&{}$#[\]\"\\!@%^+:]+"


class StageManager(SqlExecutionMixin):
    @staticmethod
    def get_standard_stage_name(name: str) -> str:
        # Handle embedded stages
        if name.startswith("snow://") or name.startswith("@"):
            return name

        return f"@{name}"

    @staticmethod
    def quote_stage_name(name: str) -> str:
        if name.startswith("'") and name.endswith("'"):
            return name  # already quoted

        standard_name = StageManager.get_standard_stage_name(name)
        if standard_name.startswith("@") and not re.fullmatch(
            r"@([\w./$])+", standard_name
        ):
            return to_string_literal(standard_name)

        return standard_name

    def _to_uri(self, local_path: str):
        uri = f"file://{local_path}"
        if re.fullmatch(UNQUOTED_FILE_URI_REGEX, uri):
            return uri
        return to_string_literal(uri)

    def list_files(self, stage_name: str) -> SnowflakeCursor:
        stage_name = self.get_standard_stage_name(stage_name)
        return self._execute_query(f"ls {self.quote_stage_name(stage_name)}")

    def get(
        self, stage_name: str, dest_path: Path, parallel: int = 4
    ) -> SnowflakeCursor:
        stage_name = self.get_standard_stage_name(stage_name)
        dest_directory = f"{dest_path}/"
        return self._execute_query(
            f"get {self.quote_stage_name(stage_name)} {self._to_uri(dest_directory)} parallel={parallel}"
        )

    def put(
        self,
        local_path: Union[str, Path],
        stage_path: str,
        parallel: int = 4,
        overwrite: bool = False,
        role: Optional[str] = None,
    ) -> SnowflakeCursor:
        """
        This method will take a file path from the user's system and put it into a Snowflake stage,
        which includes its fully qualified name as well as the path within the stage.
        If provided with a role, then temporarily use this role to perform the operation above,
        and switch back to the original role for the next commands to run.
        """
        with self.use_role(role) if role else nullcontext():
            stage_path = self.get_standard_stage_name(stage_path)
            local_resolved_path = path_resolver(str(local_path))
            log.info("Uploading %s to @%s", local_resolved_path, stage_path)
            cursor = self._execute_query(
                f"put {self._to_uri(local_resolved_path)} {self.quote_stage_name(stage_path)} "
                f"auto_compress=false parallel={parallel} overwrite={overwrite}"
            )
        return cursor

    def remove(
        self, stage_name: str, path: str, role: Optional[str] = None
    ) -> SnowflakeCursor:
        """
        This method will take a file path that exists on a Snowflake stage,
        and remove it from the stage.
        If provided with a role, then temporarily use this role to perform the operation above,
        and switch back to the original role for the next commands to run.
        """
        with self.use_role(role) if role else nullcontext():
            stage_name = self.get_standard_stage_name(stage_name)
            path = path if path.startswith("/") else "/" + path
            quoted_stage_name = self.quote_stage_name(f"{stage_name}{path}")
            return self._execute_query(f"remove {quoted_stage_name}")

    def create(self, stage_name: str, comment: Optional[str] = None) -> SnowflakeCursor:
        query = f"create stage if not exists {stage_name}"
        if comment:
            query += f" comment='{comment}'"
        return self._execute_query(query)
