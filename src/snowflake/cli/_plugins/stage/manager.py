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
import os
import re
import shutil
import sys
import time
from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass
from os import path
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from typing import Deque, Dict, Generator, List, Optional, Union

from click import ClickException, UsageError
from snowflake.cli._plugins.snowpark.package_utils import parse_requirements
from snowflake.cli.api.commands.common import (
    OnErrorType,
    Variable,
)
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import PYTHON_3_12
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import VALID_IDENTIFIER_REGEX, to_string_literal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath
from snowflake.cli.api.utils.path_utils import path_resolver, resolve_without_follow
from snowflake.connector import DictCursor, ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

if sys.version_info < PYTHON_3_12:
    # Because Snowpark works only below 3.12 and to use @sproc Session must be imported here.
    from snowflake.snowpark import Session

log = logging.getLogger(__name__)


UNQUOTED_FILE_URI_REGEX = r"[\w/*?\-.=&{}$#[\]\"\\!@%^+:]+"
USER_STAGE_PREFIX = "@~"
EXECUTE_SUPPORTED_FILES_FORMATS = (
    ".sql",
    ".py",
)  # tuple to preserve order but it's a set

# Replace magic numbers with constants
OMIT_FIRST = slice(1, None)
STAGE_PATH_REGEX = rf"(?P<prefix>(@|{re.escape('snow://')}))?(?:(?P<first_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?:(?P<second_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?P<name>{VALID_IDENTIFIER_REGEX})/?(?P<directory>([^/]*/?)*)?"


@dataclass
class StagePathParts:
    directory: str
    stage: str
    stage_name: str
    is_directory: bool

    @classmethod
    def get_directory(cls, stage_path: str) -> str:
        return "/".join(Path(stage_path).parts[OMIT_FIRST])

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def full_path(self) -> str:
        raise NotImplementedError

    @property
    def schema(self) -> str | None:
        raise NotImplementedError

    def replace_stage_prefix(self, file_path: str) -> str:
        raise NotImplementedError

    def add_stage_prefix(self, file_path: str) -> str:
        raise NotImplementedError

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        raise NotImplementedError

    def get_full_stage_path(self, path: str):
        if prefix := FQN.from_stage_path(self.stage).prefix:
            return prefix + "." + path
        return path

    def get_standard_stage_path(self) -> str:
        path = self.get_full_stage_path(self.path)
        return f"@{path}{'/'if self.is_directory and not path.endswith('/') else ''}"

    def get_standard_stage_directory_path(self) -> str:
        path = self.get_standard_stage_path()
        if not path.endswith("/"):
            return path + "/"
        return path

    def strip_stage_prefix(self, path: str):
        raise NotImplementedError


def _strip_standard_stage_prefix(path: str) -> str:
    """Removes '@' or 'snow://' prefix from given string"""
    for prefix in ["@", "snow://"]:
        if path.startswith(prefix):
            path = path.removeprefix(prefix)
    return path


@dataclass
class DefaultStagePathParts(StagePathParts):
    """
    For path like @db.schema.stage/dir the values will be:
        directory = dir
        stage = @db.schema.stage
        stage_name = stage
    For `snow://stage/dir` to
        stage -> snow://stage
        stage_name -> stage
        directory -> dir
    """

    def __init__(self, stage_path: str):
        match = re.fullmatch(STAGE_PATH_REGEX, stage_path)
        if match is None:
            raise ClickException("Invalid stage path")
        self.directory = match.group("directory")
        self._schema = match.group("second_qualifier") or match.group("first_qualifier")
        self._prefix = match.group("prefix") or "@"
        self.stage = stage_path.removesuffix(self.directory).rstrip("/")

        stage_name = FQN.from_stage(self.stage).name
        if stage_name.startswith(self._prefix):
            stage_name = stage_name.removeprefix(self._prefix)
        self.stage_name = stage_name
        self.is_directory = True if stage_path.endswith("/") else False

    @classmethod
    def from_fqn(
        cls, stage_fqn: str, subdir: str | None = None
    ) -> DefaultStagePathParts:
        full_path = f"{stage_fqn}/{subdir}" if subdir else stage_fqn
        return cls(full_path)

    @property
    def path(self) -> str:
        return f"{self.stage_name.rstrip('/')}/{self.directory}".rstrip("/")

    @property
    def full_path(self) -> str:
        return f"{self.stage.rstrip('/')}/{self.directory}".rstrip("/")

    @property
    def schema(self) -> str | None:
        return self._schema

    def replace_stage_prefix(self, file_path: str) -> str:
        file_path = _strip_standard_stage_prefix(file_path)
        file_path_without_prefix = Path(file_path).parts[OMIT_FIRST]
        return f"{self.stage}/{'/'.join(file_path_without_prefix)}"

    def strip_stage_prefix(self, file_path: str) -> str:
        file_path = _strip_standard_stage_prefix(file_path)
        if file_path.startswith(self.stage_name):
            return file_path[len(self.stage_name) :]
        return file_path

    def add_stage_prefix(self, file_path: str) -> str:
        stage = self.stage.rstrip("/")
        return f"{stage}/{file_path.lstrip('/')}"

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
        self.stage = USER_STAGE_PREFIX
        self.stage_name = USER_STAGE_PREFIX
        self.is_directory = True if stage_path.endswith("/") else False

    @classmethod
    def get_directory(cls, stage_path: str) -> str:
        if Path(stage_path).parts[0] == USER_STAGE_PREFIX:
            return super().get_directory(stage_path)
        return stage_path

    @property
    def path(self) -> str:
        return f"{self.directory}"

    @property
    def full_path(self) -> str:
        return f"{self.stage}/{self.directory}".rstrip("/")

    def replace_stage_prefix(self, file_path: str) -> str:
        if Path(file_path).parts[0] == self.stage_name:
            return file_path
        return f"{self.stage}/{file_path}"

    def add_stage_prefix(self, file_path: str) -> str:
        return f"{self.stage}/{file_path}"

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        stage_path_length = len(Path(self.directory).parts)
        return list(Path(file_path).parts[stage_path_length:-1])


class StageManager(SqlExecutionMixin):
    def __init__(self):
        super().__init__()
        self._python_exe_procedure = None

    @staticmethod
    def build_path(stage_path: str) -> StagePath:
        return StagePath.from_stage_str(stage_path)

    @staticmethod
    def get_standard_stage_prefix(name: str | FQN) -> str:
        if isinstance(name, FQN):
            name = name.identifier
        # Handle embedded stages
        if name.startswith("snow://") or name.startswith("@"):
            return name

        return f"@{name}"

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

    def list_files(
        self, stage_name: str | StagePath, pattern: str | None = None
    ) -> DictCursor:
        if not isinstance(stage_name, StagePath):
            stage_path = self.build_path(stage_name).path_for_sql()
        else:
            stage_path = stage_name.path_for_sql()
        query = f"ls {stage_path}"
        if pattern is not None:
            query += f" pattern = '{pattern}'"
        return self.execute_query(query, cursor_class=DictCursor)

    @staticmethod
    def _assure_is_existing_directory(path: Path) -> None:
        spath = SecurePath(path)
        if not spath.exists():
            spath.mkdir(parents=True)
        spath.assert_is_directory()

    def get(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> SnowflakeCursor:
        spath = self.build_path(stage_path)
        self._assure_is_existing_directory(dest_path)
        dest_directory = f"{dest_path}/"
        return self.execute_query(
            f"get {spath.path_for_sql()} {self._to_uri(dest_directory)} parallel={parallel}"
        )

    def get_recursive(
        self, stage_path: str, dest_path: Path, parallel: int = 4
    ) -> List[SnowflakeCursor]:
        stage_root = self.build_path(stage_path)

        results = []
        for file_path in self.iter_stage(stage_root):
            local_dir = file_path.get_local_target_path(
                target_dir=dest_path, stage_root=stage_root
            )
            self._assure_is_existing_directory(local_dir)

            result = self.execute_query(
                f"get {file_path.path_for_sql()} {self._to_uri(f'{local_dir}/')} parallel={parallel}"
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
        use_dict_cursor: bool = False,
    ) -> SnowflakeCursor:
        """
        This method will take a file path from the user's system and put it into a Snowflake stage,
        which includes its fully qualified name as well as the path within the stage.
        If provided with a role, then temporarily use this role to perform the operation above,
        and switch back to the original role for the next commands to run.
        """
        if "*" not in str(local_path):
            local_path = (
                os.path.join(local_path, "*")
                if Path(local_path).is_dir()
                else str(local_path)
            )
        with self.use_role(role) if role else nullcontext():
            spath = self.build_path(stage_path)
            local_resolved_path = path_resolver(str(local_path))
            log.info("Uploading %s to %s", local_resolved_path, stage_path)
            cursor = self.execute_query(
                f"put {self._to_uri(local_resolved_path)} {spath.path_for_sql()} "
                f"auto_compress={str(auto_compress).lower()} parallel={parallel} overwrite={overwrite}",
                cursor_class=DictCursor if use_dict_cursor else SnowflakeCursor,
            )
        return cursor

    @staticmethod
    def _symlink_or_copy(source_root: Path, source_file_or_dir: Path, dest_dir: Path):

        absolute_src = resolve_without_follow(source_file_or_dir)
        dest_path = dest_dir / source_file_or_dir.relative_to(source_root)

        if absolute_src.is_file():
            try:
                os.symlink(absolute_src, dest_path)
            except OSError:
                if not dest_path.parent.exists():
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(absolute_src, dest_path)
        else:
            dest_path.mkdir(exist_ok=True, parents=True)

    def put_recursive(
        self,
        local_path: Path,
        stage_path: str,
        parallel: int = 4,
        overwrite: bool = False,
        role: Optional[str] = None,
        auto_compress: bool = False,
    ) -> Generator[dict, None, None]:
        if local_path.is_file():
            raise UsageError("Cannot use recursive upload with a single file.")

        if local_path.is_dir():
            root = local_path
            glob_pattern = str(local_path / "**/*")
        else:
            root = Path([p for p in local_path.parents if p.is_dir()][0])
            glob_pattern = str(local_path)

        with TemporaryDirectory() as tmp:
            temp_dir_with_copy = Path(tmp)

            # Create a symlink or copy the file to the temp directory
            for file_or_dir in glob.iglob(glob_pattern, recursive=True):
                self._symlink_or_copy(
                    source_root=root,
                    source_file_or_dir=Path(file_or_dir),
                    dest_dir=temp_dir_with_copy,
                )

            # Find the deepest directories, we will be iterating from bottom to top
            deepest_dirs_list = self._find_deepest_directories(temp_dir_with_copy)

            while deepest_dirs_list:
                # Remove as visited
                directory = deepest_dirs_list.pop(0)

                # We reached root but there are still directories to process
                if directory == temp_dir_with_copy and deepest_dirs_list:
                    continue

                # Upload the directory content, at this moment the directory has only files
                if list(directory.iterdir()):
                    destination = StagePath.from_stage_str(
                        stage_path
                    ) / directory.relative_to(temp_dir_with_copy)
                    results: list[dict] = self.put(
                        local_path=directory,
                        stage_path=destination,
                        parallel=parallel,
                        overwrite=overwrite,
                        role=role,
                        auto_compress=auto_compress,
                        use_dict_cursor=True,
                    ).fetchall()

                    # Rewrite results to have resolved paths for better UX
                    for item in results:
                        item["source"] = (directory / item["source"]).relative_to(
                            temp_dir_with_copy
                        )
                        item["target"] = str(destination / item["target"])
                        yield item

                # We end if we reach the root directory
                if directory == temp_dir_with_copy:
                    break
                # Add parent directory to the list if it's not already there
                if directory.parent not in deepest_dirs_list and not any(
                    (
                        existing_dir.is_relative_to(directory.parent)
                        for existing_dir in deepest_dirs_list
                    )
                ):
                    deepest_dirs_list.append(directory.parent)

                # Remove the directory so the parent directory will contain only files
                shutil.rmtree(directory)

    @staticmethod
    def _find_deepest_directories(root_directory: Path) -> list[Path]:
        """
        BFS to find the deepest directories. Build a tree of directories
        structure and return leaves.
        """
        deepest_dirs: list[Path] = list()

        queue: Deque[Path] = deque()
        queue.append(root_directory)
        while queue:
            current_dir = queue.popleft()
            # Sorted to have deterministic order
            children_directories = sorted(
                list(d for d in current_dir.iterdir() if d.is_dir())
            )
            if not children_directories and current_dir not in deepest_dirs:
                deepest_dirs.append(current_dir)
            else:
                queue.extend([c for c in children_directories if c not in deepest_dirs])
        deepest_dirs_list = sorted(
            list(deepest_dirs), key=lambda d: len(d.parts), reverse=True
        )
        return deepest_dirs_list

    def copy_files(self, source_path: str, destination_path: str) -> SnowflakeCursor:
        source_stage_path = self.build_path(source_path)
        # We copy only into stage
        destination_stage_path = StagePath.from_stage_str(destination_path)

        if destination_stage_path.is_user_stage():
            raise ClickException(
                "Destination path cannot be a user stage. Please provide a named stage."
            )

        log.info(
            "Copying files from %s to %s", source_stage_path, destination_stage_path
        )
        # Destination needs to end with /
        dest = destination_stage_path.absolute_path().rstrip("/") + "/"
        query = f"copy files into {dest} from {source_stage_path}"
        return self.execute_query(query)

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
            stage_path = self.build_path(stage_name) / path
            return self.execute_query(f"remove {stage_path.path_for_sql()}")

    def create(
        self, fqn: FQN, comment: Optional[str] = None, temporary: bool = False
    ) -> SnowflakeCursor:
        temporary_str = "temporary " if temporary else ""
        query = f"create {temporary_str}stage if not exists {fqn.sql_identifier}"
        if comment:
            query += f" comment='{comment}'"
        return self.execute_query(query)

    def iter_stage(self, stage_path: StagePath):
        for file in self.list_files(stage_path.absolute_path()).fetchall():
            if stage_path.is_user_stage():
                path = StagePath.get_user_stage() / file["name"]
            else:
                path = self.build_path(file["name"])
            yield path

    def execute(
        self,
        stage_path_str: str,
        on_error: OnErrorType,
        variables: Optional[List[str]] = None,
        requires_temporary_stage: bool = False,
    ):
        if requires_temporary_stage:
            (
                stage_path_parts,
                original_path_parts,
            ) = self._create_temporary_copy_of_stage(stage_path_str)
            stage_path = StagePath.from_stage_str(
                stage_path_parts.get_standard_stage_path()
            )
        else:
            stage_path_parts = self.stage_path_parts_from_str(stage_path_str)
            stage_path = self.build_path(stage_path_str)

        all_files_list = self._get_files_list_from_stage(stage_path.root_path())
        if not all_files_list:
            raise ClickException(f"No files found on stage '{stage_path}'")

        all_files_with_stage_name_prefix = [
            stage_path_parts.get_directory(file) for file in all_files_list
        ]

        # filter files from stage if match stage_path pattern
        filtered_file_list = self._filter_files_list(
            stage_path_parts, all_files_with_stage_name_prefix
        )

        if not filtered_file_list:
            raise ClickException(f"No files matched pattern '{stage_path}'")

        # sort filtered files in alphabetical order with directories at the end
        sorted_file_path_list = sorted(
            filtered_file_list, key=lambda f: (path.dirname(f), path.basename(f))
        )

        parsed_variables = parse_key_value_variables(variables)
        sql_variables = self.parse_execute_variables(parsed_variables)
        python_variables = self._parse_python_variables(parsed_variables)
        results = []

        if any(file.endswith(".py") for file in sorted_file_path_list):
            self._python_exe_procedure = self._bootstrap_snowpark_execution_environment(
                stage_path
            )

        for file_path in sorted_file_path_list:
            file_stage_path = stage_path_parts.add_stage_prefix(file_path)

            # For better reporting push down the information about original
            # path if execution happens from temporary stage
            if requires_temporary_stage:
                original_path = original_path_parts.add_stage_prefix(file_path)
            else:
                original_path = file_stage_path

            if file_path.endswith(".py"):
                result = self._execute_python(
                    file_stage_path=file_stage_path,
                    on_error=on_error,
                    variables=python_variables,
                    original_file=original_path,
                )
            else:
                result = self._call_execute_immediate(
                    file_stage_path=file_stage_path,
                    variables=sql_variables,
                    on_error=on_error,
                    original_file=original_path,
                )
            results.append(result)

        return results

    def _create_temporary_copy_of_stage(
        self, stage_path: str
    ) -> tuple[StagePathParts, StagePathParts]:
        sm = StageManager()

        # Rewrite stage paths to temporary stage paths. Git paths become stage paths
        original_path_parts = self.stage_path_parts_from_str(stage_path)  # noqa: SLF001

        tmp_stage_name = f"snowflake_cli_tmp_stage_{int(time.time())}"
        tmp_stage_fqn = FQN.from_stage(tmp_stage_name).using_connection(conn=self._conn)
        tmp_stage = tmp_stage_fqn.identifier
        stage_path_parts = sm.stage_path_parts_from_str(  # noqa: SLF001
            tmp_stage + "/" + original_path_parts.directory
        )

        # Create temporary stage, it will be dropped with end of session
        sm.create(tmp_stage_fqn, temporary=True)

        # Copy the content
        self.copy_files(
            source_path=original_path_parts.get_full_stage_path(
                original_path_parts.stage_name
            ),
            destination_path=stage_path_parts.get_full_stage_path(
                stage_path_parts.stage_name
            ),
        )
        return stage_path_parts, original_path_parts

    def _get_files_list_from_stage(
        self, stage_path: StagePath, pattern: str | None = None
    ) -> List[str]:
        files_list_result = self.list_files(stage_path, pattern=pattern).fetchall()
        return [f["name"] for f in files_list_result]

    def _filter_files_list(
        self, stage_path_parts: StagePathParts, files_on_stage: List[str]
    ) -> List[str]:
        if not stage_path_parts.directory:
            return self._filter_supported_files(files_on_stage)

        stage_path = stage_path_parts.directory

        # Exact file path was provided if stage_path in file list
        if stage_path in files_on_stage:
            filtered_files = self._filter_supported_files([stage_path])
            if filtered_files:
                return filtered_files
            else:
                raise ClickException(
                    f"Invalid file extension, only {', '.join(EXECUTE_SUPPORTED_FILES_FORMATS)} files are allowed."
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
    def parse_execute_variables(variables: List[Variable]) -> Optional[str]:
        if not variables:
            return None
        query_parameters = [f"{v.key}=>{v.value}" for v in variables]
        return f" using ({', '.join(query_parameters)})"

    @staticmethod
    def _parse_python_variables(variables: List[Variable]) -> Dict:
        def _unwrap(s: str):
            if s.startswith("'") and s.endswith("'"):
                return s[1:-1]
            if s.startswith('"') and s.endswith('"'):
                return s[1:-1]
            return s

        return {str(v.key): _unwrap(v.value) for v in variables}

    @staticmethod
    def _success_result(file: str):
        cli_console.warning(f"SUCCESS - {file}")
        return {"File": file, "Status": "SUCCESS", "Error": None}

    @staticmethod
    def _error_result(file: str, msg: str):
        cli_console.warning(f"FAILURE - {file}")
        return {"File": file, "Status": "FAILURE", "Error": msg}

    @staticmethod
    def _handle_execution_exception(on_error: OnErrorType, exception: Exception):
        if on_error == OnErrorType.BREAK:
            raise exception

    def _call_execute_immediate(
        self,
        file_stage_path: str,
        variables: Optional[str],
        on_error: OnErrorType,
        original_file: str,
    ) -> Dict:
        try:
            log.info("Executing SQL file: %s", file_stage_path)
            query = f"execute immediate from {self.quote_stage_name(file_stage_path)}"
            if variables:
                query += variables
            self.execute_query(query)
            return StageManager._success_result(file=original_file)
        except ProgrammingError as e:
            StageManager._handle_execution_exception(on_error=on_error, exception=e)
            return StageManager._error_result(file=original_file, msg=e.msg)

    @staticmethod
    def stage_path_parts_from_str(stage_path: str) -> StagePathParts:
        """Create StagePathParts object from stage path string."""
        stage_path = StageManager.get_standard_stage_prefix(stage_path)
        if stage_path.startswith(USER_STAGE_PREFIX):
            return UserStagePathParts(stage_path)
        return DefaultStagePathParts(stage_path)

    def _check_for_requirements_file(self, stage_path: StagePath) -> List[str]:
        """Looks for requirements.txt file on stage."""
        current_dir = stage_path.parent if stage_path.is_file() else stage_path
        req_files_on_stage = self._get_files_list_from_stage(
            current_dir, pattern=r".*requirements\.txt$"
        )
        if not req_files_on_stage:
            return []

        # Construct all possible path for requirements file for this context
        req_file_name = "requirements.txt"
        possible_req_files = []
        while not current_dir.is_root():
            current_file = current_dir / req_file_name
            possible_req_files.append(current_file)
            current_dir = current_dir.parent

        current_file = current_dir / req_file_name
        possible_req_files.append(current_file)

        # Now for every possible path check if the file exists on stage,
        # if yes break, we use the first possible file
        requirements_file: StagePath | None = None
        for req_file in possible_req_files:
            if (
                req_file.absolute_path(no_fqn=True, at_prefix=False)
                in req_files_on_stage
            ):
                requirements_file = req_file
                break

        # If we haven't found any matching requirements
        if requirements_file is None:
            return []

        # req_file at this moment is the first found requirements file
        requirements_path = requirements_file.with_stage(stage_path.stage)
        with SecurePath.temporary_directory() as tmp_dir:
            self.get(str(requirements_path), tmp_dir.path)
            requirements = parse_requirements(
                requirements_file=tmp_dir / "requirements.txt"
            )

        return [req.package_name for req in requirements]

    def _bootstrap_snowpark_execution_environment(self, stage_path: StagePath):
        """Prepares Snowpark session for executing Python code remotely."""
        if sys.version_info >= PYTHON_3_12:
            raise ClickException(
                f"Executing Python files is not supported in Python >= 3.12. Current version: {sys.version}"
            )

        from snowflake.snowpark.functions import sproc

        self.snowpark_session.add_packages("snowflake-snowpark-python")
        self.snowpark_session.add_packages("snowflake.core")
        requirements = self._check_for_requirements_file(stage_path)
        self.snowpark_session.add_packages(*requirements)

        @sproc(is_permanent=False, session=self.snowpark_session)
        def _python_execution_procedure(
            _: Session, file_path: str, variables: Dict | None = None
        ) -> None:
            """Snowpark session-scoped stored procedure to execute content of provided Python file."""
            import json

            from snowflake.snowpark.files import SnowflakeFile

            with SnowflakeFile.open(file_path, require_scoped_url=False) as f:
                file_content: str = f.read()  # type: ignore

            wrapper = dedent(
                f"""\
                import os
                os.environ.update({json.dumps(variables)})
                """
            )

            exec(wrapper + file_content)

        return _python_execution_procedure

    def _execute_python(
        self,
        file_stage_path: str,
        on_error: OnErrorType,
        variables: Dict,
        original_file: str,
    ):
        """
        Executes Python file from stage using a Snowpark temporary procedure.
        Currently, there's no option to pass input to the execution.
        """
        from snowflake.snowpark.exceptions import SnowparkSQLException

        try:
            log.info("Executing Python file: %s", file_stage_path)
            self._python_exe_procedure(self.get_standard_stage_prefix(file_stage_path), variables, session=self.snowpark_session)  # type: ignore
            return StageManager._success_result(file=original_file)
        except SnowparkSQLException as e:
            StageManager._handle_execution_exception(on_error=on_error, exception=e)
            return StageManager._error_result(file=original_file, msg=e.message)
