# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
USER_STAGE_PREFIX = "@~"


@dataclass
class StagePathParts:
    directory: str
    stage: str
    stage_name: str

    @staticmethod
    def get_directory(stage_path: str) -> str:
        return "/".join(Path(stage_path).parts[1:])

    @property
    def path(self) -> str:
        raise NotImplementedError

    def add_stage_prefix(self, file_path: str) -> str:
        raise NotImplementedError

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        raise NotImplementedError


@dataclass
class DefaultStagePathParts(StagePathParts):
    """
    For path like @db.schema.stage/dir the values will be:
        directory = dir
        stage = @db.schema.stage
        stage_name = stage
    For `@stage/dir` to
        stage -> @stage
        stage_name -> stage
        directory -> dir
    """

    def __init__(self, stage_path: str):
        self.directory = self.get_directory(stage_path)
        self.stage = StageManager.get_stage_from_path(stage_path)
        stage_name = self.stage.split(".")[-1]
        if stage_name.startswith("@"):
            stage_name = stage_name[1:]
        self.stage_name = stage_name

    @property
    def path(self) -> str:
        return (
            f"{self.stage_name}{self.directory}".lower()
            if self.stage_name.endswith("/")
            else f"{self.stage_name}/{self.directory}".lower()
        )

    def add_stage_prefix(self, file_path: str) -> str:
        stage = Path(self.stage).parts[0]
        file_path_without_prefix = Path(file_path).parts[1:]
        return f"{stage}/{'/'.join(file_path_without_prefix)}"

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        stage_path_length = len(Path(self.directory).parts)
        return list(Path(file_path).parts[1 + stage_path_length : -1])


@dataclass
class UserStagePathParts(StagePathParts):
    """
    For path like @db.schema.stage/dir the values will be:
        directory = dir
        stage = @~
        stage_name = @~
    """

    def __init__(self, stage_path: str):
        self.directory = self.get_directory(stage_path)
        self.stage = "@~"
        self.stage_name = "@~"

    @property
    def path(self) -> str:
        return f"{self.directory}".lower()

    def add_stage_prefix(self, file_path: str) -> str:
        return f"{self.stage}/{file_path}"

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        stage_path_length = len(Path(self.directory).parts)
        return list(Path(file_path).parts[stage_path_length:-1])


class StageManager(SqlExecutionMixin):
    @staticmethod
    def get_standard_stage_prefix(name: str) -> str:
        # Handle embedded stages
        if name.startswith("snow://") or name.startswith("@"):
            return name

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
        stage_path_parts = self._stage_path_part_factory(stage_path)

        results = []
        for file_path in self.iter_stage(stage_path):
            dest_directory = dest_path
            for path_part in stage_path_parts.get_directory_from_file_path(file_path):
                dest_directory = dest_directory / path_part
            self._assure_is_existing_directory(dest_directory)

            result = self._execute_query(
                f"get {self.quote_stage_name(stage_path_parts.add_stage_prefix(file_path))} {self._to_uri(f'{dest_directory}/')} parallel={parallel}"
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
        auto_compress: bool = False,
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
            log.info("Uploading %s to %s", local_resolved_path, stage_path)
            cursor = self._execute_query(
                f"put {self._to_uri(local_resolved_path)} {self.quote_stage_name(stage_path)} "
                f"auto_compress={str(auto_compress).lower()} parallel={parallel} overwrite={overwrite}"
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
        stage_path_parts = self._stage_path_part_factory(stage_path)
        all_files_list = self._get_files_list_from_stage(stage_path_parts)

        # filter files from stage if match stage_path pattern
        filtered_file_list = self._filter_files_list(stage_path_parts, all_files_list)

        if not filtered_file_list:
            raise ClickException(f"No files matched pattern '{stage_path}'")

        # sort filtered files in alphabetical order with directories at the end
        sorted_file_path_list = sorted(
            filtered_file_list, key=lambda f: (path.dirname(f), path.basename(f))
        )

        sql_variables = self._parse_execute_variables(variables)
        results = []
        for file_path in sorted_file_path_list:
            results.append(
                self._call_execute_immediate(
                    stage_path_parts=stage_path_parts,
                    file_path=file_path,
                    variables=sql_variables,
                    on_error=on_error,
                )
            )

        return results

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
        file_path: str,
        variables: Optional[str],
        on_error: OnErrorType,
    ) -> Dict:
        file_stage_path = stage_path_parts.add_stage_prefix(file_path)
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

    @staticmethod
    def _stage_path_part_factory(stage_path: str) -> StagePathParts:
        stage_path = StageManager.get_standard_stage_prefix(stage_path)
        if stage_path.startswith(USER_STAGE_PREFIX):
            return UserStagePathParts(stage_path)
        return DefaultStagePathParts(stage_path)
