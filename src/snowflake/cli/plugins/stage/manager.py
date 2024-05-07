from __future__ import annotations

import fnmatch
import glob
import logging
import re
from contextlib import nullcontext
from dataclasses import dataclass
from os import path
from pathlib import Path
from typing import Dict, List, Optional, Union

from click import ClickException
from snowflake.cli.api.commands.flags import OnErrorType, parse_key_value_variables
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.utils.path_utils import path_resolver
from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


UNQUOTED_FILE_URI_REGEX = r"[\w/*?\-.=&{}$#[\]\"\\!@%^+:]+"
EXECUTE_SUPPORTED_FILES_FORMATS = {".sql"}


@dataclass
class StagePathParts:
    # For path like @db.schema.stage/dir the values will be:
    # stage = @db.schema.stage
    stage: str
    # stage_name = stage/dir
    stage_name: str
    # directory = dir
    directory: str

    @property
    def path(self) -> str:
        return (
            f"{self.stage_name}{self.directory}".lower()
            if self.stage_name.endswith("/")
            else f"{self.stage_name}/{self.directory}".lower()
        )


class StageManager(SqlExecutionMixin):
    @staticmethod
    def get_standard_stage_prefix(name: str) -> str:
        # Handle embedded stages
        if name.startswith("@"):
            return name

        if name.startswith("snow://"):
            return f"@{name[7:]}"

        return f"@{name}"

    @staticmethod
    def get_standard_stage_directory_path(path):
        if not path.endswith("/"):
            path += "/"
        return StageManager.get_standard_stage_prefix(path)

    @staticmethod
    def get_stage_from_path(path: str):
        """
        Returns stage name from potential path on stage. For example
        db.schema.stage/foo/bar  -> db.schema.stage
        """
        return Path(path).parts[0]

    @staticmethod
    def quote_stage_name(name: str) -> str:
        if name.startswith("'") and name.endswith("'"):
            return name  # already quoted

        standard_name = StageManager.get_standard_stage_prefix(name)
        if standard_name.startswith("@") and not re.fullmatch(
            r"@([\w./$])+", standard_name
        ):
            return to_string_literal(standard_name)

        return standard_name

    @staticmethod
    def remove_stage_prefix(stage_path: str) -> str:
        if stage_path.startswith("@"):
            return stage_path[1:]
        return stage_path

    def _to_uri(self, local_path: str):
        uri = f"file://{local_path}"
        if re.fullmatch(UNQUOTED_FILE_URI_REGEX, uri):
            return uri
        return to_string_literal(uri)

    def list_files(self, stage_name: str, pattern: str | None = None) -> DictCursor:
        stage_name = self.get_standard_stage_prefix(stage_name)
        query = f"ls {self.quote_stage_name(stage_name)}"
        if pattern is not None:
            query += f" pattern = '{pattern}'"
        return self._execute_query(query, cursor_class=DictCursor)

    @staticmethod
    def _assure_is_existing_directory(path: Path) -> None:
        spath = SecurePath(path)
        if not spath.exists():
            spath.mkdir(parents=True)
        spath.assert_is_directory()

    def get(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> SnowflakeCursor:
        stage_path = self.get_standard_stage_prefix(stage_path)
        self._assure_is_existing_directory(dest_path)
        dest_directory = f"{dest_path}/"
        return self._execute_query(
            f"get {self.quote_stage_name(stage_path)} {self._to_uri(dest_directory)} parallel={parallel}"
        )

    def get_recursive(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> List[SnowflakeCursor]:
        stage_path = self.get_standard_stage_prefix(stage_path)
        stage_parts_length = len(Path(stage_path).parts)

        results = []
        for file in self.iter_stage(stage_path):
            dest_directory = dest_path / "/".join(
                Path(file).parts[stage_parts_length:-1]
            )
            self._assure_is_existing_directory(Path(dest_directory))

            stage_path_with_prefix = self.get_standard_stage_prefix(file)

            result = self._execute_query(
                f"get {self.quote_stage_name(stage_path_with_prefix)} {self._to_uri(f'{dest_directory}/')} parallel={parallel}"
            )
            results.append(result)

        return results

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
            stage_path = self.get_standard_stage_prefix(stage_path)
            local_resolved_path = path_resolver(str(local_path))
            log.info("Uploading %s to @%s", local_resolved_path, stage_path)
            cursor = self._execute_query(
                f"put {self._to_uri(local_resolved_path)} {self.quote_stage_name(stage_path)} "
                f"auto_compress=false parallel={parallel} overwrite={overwrite}"
            )
        return cursor

    def copy_files(self, source_path: str, destination_path: str) -> SnowflakeCursor:
        source = self.get_standard_stage_prefix(source_path)
        destination = self.get_standard_stage_directory_path(destination_path)
        log.info("Copying files from %s to %s", source, destination)
        query = f"copy files into {destination} from {source}"
        return self._execute_query(query)

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
            stage_name = self.get_standard_stage_prefix(stage_name)
            path = path if path.startswith("/") else "/" + path
            quoted_stage_name = self.quote_stage_name(f"{stage_name}{path}")
            return self._execute_query(f"remove {quoted_stage_name}")

    def create(self, stage_name: str, comment: Optional[str] = None) -> SnowflakeCursor:
        query = f"create stage if not exists {stage_name}"
        if comment:
            query += f" comment='{comment}'"
        return self._execute_query(query)

    def iter_stage(self, stage_path: str):
        for file in self.list_files(stage_path).fetchall():
            yield file["name"]

    def execute(
        self,
        stage_path: str,
        on_error: OnErrorType,
        variables: Optional[List[str]] = None,
    ):
        stage_path_with_prefix = self.get_standard_stage_prefix(stage_path)
        stage_path_parts = self._split_stage_path(stage_path_with_prefix)
        all_files_list = self._get_files_list_from_stage(stage_path_parts)

        # filter files from stage if match stage_path pattern
        filtered_file_list = self._filter_files_list(stage_path_parts, all_files_list)

        if not filtered_file_list:
            raise ClickException(f"No files matched pattern '{stage_path}'")

        # sort filtered files in alphabetical order with directories at the end
        sorted_file_list = sorted(
            filtered_file_list, key=lambda f: (path.dirname(f), path.basename(f))
        )

        sql_variables = self._parse_execute_variables(variables)
        results = []
        for file in sorted_file_list:
            results.append(
                self._call_execute_immediate(
                    stage_path_parts=stage_path_parts,
                    file=file,
                    variables=sql_variables,
                    on_error=on_error,
                )
            )

        return results

    def _split_stage_path(self, stage_path: str) -> StagePathParts:
        """
        Splits stage path `@stage/dir` to
            stage -> @stage
            stage_name -> stage
            directory -> dir
        For stage path with fully qualified name `@db.schema.stage/dir`
            stage -> @db.schema.stage
            stage_name -> stage
            directory -> dir
        """
        stage = self.get_stage_from_path(stage_path)
        stage_name = stage.split(".")[-1]
        if stage_name.startswith("@"):
            stage_name = stage_name[1:]
        directory = "/".join(Path(stage_path).parts[1:])
        return StagePathParts(stage, stage_name, directory)

    def _get_files_list_from_stage(self, stage_path_parts: StagePathParts) -> List[str]:
        files_list_result = self.list_files(stage_path_parts.stage).fetchall()

        if not files_list_result:
            raise ClickException(f"No files found on stage '{stage_path_parts.stage}'")

        return [f["name"] for f in files_list_result]

    def _filter_files_list(
        self, stage_path_parts: StagePathParts, files_on_stage: List[str]
    ) -> List[str]:
        if not stage_path_parts.directory:
            return self._filter_supported_files(files_on_stage)

        stage_path = stage_path_parts.path

        # Exact file path was provided if stage_path in file list
        if stage_path in files_on_stage:
            filtered_files = self._filter_supported_files([stage_path])
            if filtered_files:
                return filtered_files
            else:
                raise ClickException(
                    "Invalid file extension, only `.sql` files are allowed."
                )

        # Filter with fnmatch if contains `*` or `?`
        if glob.has_magic(stage_path):
            filtered_files = fnmatch.filter(files_on_stage, stage_path)
        else:
            # Path to directory was provided
            filtered_files = fnmatch.filter(files_on_stage, f"{stage_path}*")
        return self._filter_supported_files(filtered_files)

    @staticmethod
    def _filter_supported_files(files: List[str]) -> List[str]:
        return [f for f in files if Path(f).suffix in EXECUTE_SUPPORTED_FILES_FORMATS]

    @staticmethod
    def _parse_execute_variables(variables: Optional[List[str]]) -> Optional[str]:
        if not variables:
            return None

        parsed_variables = parse_key_value_variables(variables)
        query_parameters = [f"{v.key}=>{v.value}" for v in parsed_variables]
        return f" using ({', '.join(query_parameters)})"

    def _call_execute_immediate(
        self,
        stage_path_parts: StagePathParts,
        file: str,
        variables: Optional[str],
        on_error: OnErrorType,
    ) -> Dict:
        file_stage_path = self._build_file_stage_path(stage_path_parts, file)
        try:
            query = f"execute immediate from {file_stage_path}"
            if variables:
                query += variables
            self._execute_query(query)
            cli_console.step(f"SUCCESS - {file_stage_path}")
            return {"File": file_stage_path, "Status": "SUCCESS", "Error": None}
        except ProgrammingError as e:
            cli_console.warning(f"FAILURE - {file_stage_path}")
            if on_error == OnErrorType.BREAK:
                raise e
            return {"File": file_stage_path, "Status": "FAILURE", "Error": e.msg}

    def _build_file_stage_path(
        self, stage_path_parts: StagePathParts, file: str
    ) -> str:
        stage = Path(stage_path_parts.stage).parts[0]
        file_path = Path(file).parts[1:]
        return f"{stage}/{'/'.join(file_path)}"
