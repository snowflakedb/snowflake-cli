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

from pathlib import Path, PurePosixPath
from textwrap import dedent
from typing import List

from click import UsageError
from snowflake.cli._plugins.stage.manager import (
    USER_STAGE_PREFIX,
    StageManager,
    StagePathParts,
    UserStagePathParts,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.stage_path import StagePath
from snowflake.connector.cursor import SnowflakeCursor

# Replace magic numbers with constants
OMIT_FIRST = slice(1, None)
OMIT_STAGE = slice(3, None)
OMIT_STAGE_IN_NEW_LIST_FILES = slice(2, None)
ONLY_STAGE = slice(3)


class GitStagePathParts(StagePathParts):
    def __init__(self, stage_path: str):
        self.stage = GitManager.get_stage_from_path(stage_path)
        stage_path_parts = GitManager.split_git_path(stage_path)
        git_repo_name = stage_path_parts[0].split(".")[-1]
        if git_repo_name.startswith("@"):
            git_repo_name = git_repo_name[OMIT_FIRST]
        self.stage_name = "/".join([git_repo_name, *stage_path_parts[1:3], ""])
        self.directory = "/".join(stage_path_parts[OMIT_STAGE])
        self.is_directory = True if stage_path.endswith("/") else False

    @property
    def path(self) -> str:
        return f"{self.stage_name.rstrip('/')}/{self.directory}"

    @classmethod
    def get_directory(cls, stage_path: str) -> str:
        git_path_parts = GitManager.split_git_path(stage_path)
        # New file list does not have a stage name at the beginning
        if stage_path.startswith("/"):
            return "/".join(git_path_parts[OMIT_STAGE_IN_NEW_LIST_FILES])
        else:
            return "/".join(git_path_parts[OMIT_STAGE])

    @property
    def full_path(self) -> str:
        return f"{self.stage.rstrip('/')}/{self.directory}"

    def replace_stage_prefix(self, file_path: str) -> str:
        stage = Path(self.stage).parts[0]
        file_path_without_prefix = Path(file_path).parts[OMIT_FIRST]
        return f"{stage}/{'/'.join(file_path_without_prefix)}"

    def add_stage_prefix(self, file_path: str) -> str:
        stage = self.stage.rstrip("/")
        return f"{stage}/{file_path.lstrip('/')}"

    def get_directory_from_file_path(self, file_path: str) -> List[str]:
        stage_path_length = len(Path(self.directory).parts)
        return list(Path(file_path).parts[3 + stage_path_length : -1])


class GitManager(StageManager):
    @staticmethod
    def build_path(stage_path: str) -> StagePathParts:
        return StagePath.from_git_str(stage_path)

    def show_branches(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self.execute_query(f"show git branches like '{like}' in {repo_name}")

    def show_tags(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self.execute_query(f"show git tags like '{like}' in {repo_name}")

    def fetch(self, fqn: FQN) -> SnowflakeCursor:
        return self.execute_query(f"alter git repository {fqn} fetch")

    def create(
        self, repo_name: FQN, api_integration: str, url: str, secret: str
    ) -> SnowflakeCursor:
        query = dedent(
            f"""
            create git repository {repo_name.sql_identifier}
            api_integration = {api_integration}
            origin = '{url}'
            """
        )
        if secret is not None:
            query += f"git_credentials = {secret}\n"
        return self.execute_query(query)

    @staticmethod
    def get_stage_from_path(path: str):
        """
        Returns stage name from potential path on stage. For example
        repo/branches/main/foo/bar -> repo/branches/main/
        """
        path_parts = GitManager.split_git_path(path)
        return f"{'/'.join(path_parts[ONLY_STAGE])}/"

    @staticmethod
    def stage_path_parts_from_str(stage_path: str) -> StagePathParts:
        stage_path = StageManager.get_standard_stage_prefix(stage_path)
        if stage_path.startswith(USER_STAGE_PREFIX):
            return UserStagePathParts(stage_path)
        return GitStagePathParts(stage_path)

    @staticmethod
    def split_git_path(path: str):
        # Check if path contains quotes and split it accordingly
        if '/"' in path and '"/' in path:
            if path.count('"') > 2:
                raise UsageError(
                    f'Invalid string {path}, too much " in path, expected 2.'
                )

            path_parts = path.split('"')
            before_quoted_part = GitManager._split_path_without_empty_parts(
                path_parts[0]
            )

            if path_parts[2] == "/":
                after_quoted_part = []
            else:
                after_quoted_part = GitManager._split_path_without_empty_parts(
                    path_parts[2]
                )

            return [
                *before_quoted_part,
                f'"{path_parts[1]}"',
                *after_quoted_part,
            ]
        else:
            return GitManager._split_path_without_empty_parts(path)

    @staticmethod
    def _split_path_without_empty_parts(path: str):
        return [e for e in PurePosixPath(path).parts if e != "/"]
