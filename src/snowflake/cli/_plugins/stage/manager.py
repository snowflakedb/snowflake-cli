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
import sys
from contextlib import nullcontext
from dataclasses import dataclass
from os import path
from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Optional, Union

from click import ClickException
from snowflake.cli._plugins.snowpark.package_utils import parse_requirements
from snowflake.cli.api.commands.common import (
    OnErrorType,
    Variable,
)
from snowflake.cli.api.commands.utils import parse_key_value_variables
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import PYTHON_3_12
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_string_literal
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.utils.path_utils import path_resolver
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


@dataclass
class StagePathParts:
    directory: str
    stage: str
    stage_name: str
    is_directory: bool

    @classmethod
    def get_directory(cls, stage_path: str) -> str:
        return "/".join(Path(stage_path).parts[1:])

    @property
    def path(self) -> str:
        raise NotImplementedError

    @property
    def full_path(self) -> str:
        raise NotImplementedError

    def replace_stage_prefix(self, file_path: str) -> str:
        raise NotImplementedError

    def add_stage_prefix(self, file_path: str) -> str:
        raise NotImplementedError

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        raise NotImplementedError

    def get_full_stage_path(self, path: str):
        if prefix := FQN.from_stage(self.stage).prefix:
            return prefix + "." + path
        return path

    def get_standard_stage_path(self) -> str:
        path = self.path
        return f"@{path}{'/'if self.is_directory and not path.endswith('/') else ''}"

    def get_standard_stage_directory_path(self) -> str:
        path = self.get_standard_stage_path()
        if not path.endswith("/"):
            return path + "/"
        return path


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
        stage_name = stage_name[1:] if stage_name.startswith("@") else stage_name
        self.stage_name = stage_name
        self.is_directory = True if stage_path.endswith("/") else False

    @property
    def path(self) -> str:
        return f"{self.stage_name.rstrip('/')}/{self.directory}"

    @property
    def full_path(self) -> str:
        return f"{self.stage.rstrip('/')}/{self.directory}"

    def replace_stage_prefix(self, file_path: str) -> str:
        stage = Path(self.stage).parts[0]
        file_path_without_prefix = Path(file_path).parts[1:]
        return f"{stage}/{'/'.join(file_path_without_prefix)}"

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
        return f"{self.stage}/{self.directory}"

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
                f"get {self.quote_stage_name(stage_path_parts.replace_stage_prefix(file_path))} {self._to_uri(f'{dest_directory}/')} parallel={parallel}"
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
        source_path_parts = self._stage_path_part_factory(source_path)
        destination_path_parts = self._stage_path_part_factory(destination_path)

        if isinstance(destination_path_parts, UserStagePathParts):
            raise ClickException(
                "Destination path cannot be a user stage. Please provide a named stage."
            )

        source = source_path_parts.get_standard_stage_path()
        destination = destination_path_parts.get_standard_stage_directory_path()
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

    def create(self, fqn: FQN, comment: Optional[str] = None) -> SnowflakeCursor:
        query = f"create stage if not exists {fqn.sql_identifier}"
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
        sql_variables = self._parse_execute_variables(parsed_variables)
        python_variables = {str(v.key): v.value for v in parsed_variables}
        results = []

        if any(file.endswith(".py") for file in sorted_file_path_list):
            self._python_exe_procedure = self._bootstrap_snowpark_execution_environment(
                stage_path_parts
            )

        for file_path in sorted_file_path_list:
            file_stage_path = stage_path_parts.add_stage_prefix(file_path)
            if file_path.endswith(".py"):
                result = self._execute_python(
                    file_stage_path=file_stage_path,
                    on_error=on_error,
                    variables=python_variables,
                )
            else:
                result = self._call_execute_immediate(
                    file_stage_path=file_stage_path,
                    variables=sql_variables,
                    on_error=on_error,
                )
            results.append(result)

        return results

    def _get_files_list_from_stage(
        self, stage_path_parts: StagePathParts, pattern: str | None = None
    ) -> List[str]:
        files_list_result = self.list_files(
            stage_path_parts.stage, pattern=pattern
        ).fetchall()

        if not files_list_result:
            raise ClickException(f"No files found on stage '{stage_path_parts.stage}'")

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
    def _parse_execute_variables(variables: List[Variable]) -> Optional[str]:
        if not variables:
            return None
        query_parameters = [f"{v.key}=>{v.value}" for v in variables]
        return f" using ({', '.join(query_parameters)})"

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
    ) -> Dict:
        try:
            query = f"execute immediate from {file_stage_path}"
            if variables:
                query += variables
            self._execute_query(query)
            return StageManager._success_result(file=file_stage_path)
        except ProgrammingError as e:
            StageManager._handle_execution_exception(on_error=on_error, exception=e)
            return StageManager._error_result(file=file_stage_path, msg=e.msg)

    @staticmethod
    def _stage_path_part_factory(stage_path: str) -> StagePathParts:
        stage_path = StageManager.get_standard_stage_prefix(stage_path)
        if stage_path.startswith(USER_STAGE_PREFIX):
            return UserStagePathParts(stage_path)
        return DefaultStagePathParts(stage_path)

    def _check_for_requirements_file(
        self, stage_path_parts: StagePathParts
    ) -> List[str]:
        """Looks for requirements.txt file on stage."""
        req_files_on_stage = self._get_files_list_from_stage(
            stage_path_parts, pattern=r".*requirements\.txt$"
        )
        if not req_files_on_stage:
            return []

        # Construct all possible path for requirements file for this context
        # We don't use os.path or pathlib to preserve compatibility on Windows
        req_file_name = "requirements.txt"
        path_parts = stage_path_parts.path.split("/")
        possible_req_files = []

        while path_parts:
            current_file = "/".join([*path_parts, req_file_name])
            possible_req_files.append(str(current_file))
            path_parts = path_parts[:-1]

        # Now for every possible path check if the file exists on stage,
        # if yes break, we use the first possible file
        requirements_file = None
        for req_file in possible_req_files:
            if req_file in req_files_on_stage:
                requirements_file = req_file
                break

        # If we haven't found any matching requirements
        if requirements_file is None:
            return []

        # req_file at this moment is the first found requirements file
        with SecurePath.temporary_directory() as tmp_dir:
            self.get(
                stage_path_parts.get_full_stage_path(requirements_file), tmp_dir.path
            )
            requirements = parse_requirements(
                requirements_file=tmp_dir / "requirements.txt"
            )

        return [req.package_name for req in requirements]

    def _bootstrap_snowpark_execution_environment(
        self, stage_path_parts: StagePathParts
    ):
        """Prepares Snowpark session for executing Python code remotely."""
        if sys.version_info >= PYTHON_3_12:
            raise ClickException(
                f"Executing python files is not supported in Python >= 3.12. Current version: {sys.version}"
            )

        from snowflake.snowpark.functions import sproc

        self.snowpark_session.add_packages("snowflake-snowpark-python")
        self.snowpark_session.add_packages("snowflake.core")
        requirements = self._check_for_requirements_file(stage_path_parts)
        self.snowpark_session.add_packages(*requirements)

        @sproc(is_permanent=False)
        def _python_execution_procedure(
            _: Session, file_path: str, variables: Dict | None = None
        ) -> None:
            """Snowpark session-scoped stored procedure to execute content of provided python file."""
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
        self, file_stage_path: str, on_error: OnErrorType, variables: Dict
    ):
        """
        Executes Python file from stage using a Snowpark temporary procedure.
        Currently, there's no option to pass input to the execution.
        """
        from snowflake.snowpark.exceptions import SnowparkSQLException

        try:
            self._python_exe_procedure(self.get_standard_stage_prefix(file_stage_path), variables)  # type: ignore
            return StageManager._success_result(file=file_stage_path)
        except SnowparkSQLException as e:
            StageManager._handle_execution_exception(on_error=on_error, exception=e)
            return StageManager._error_result(file=file_stage_path, msg=e.message)
