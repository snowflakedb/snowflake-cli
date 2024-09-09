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

from pathlib import Path
from textwrap import dedent
from typing import List

from snowflake.cli._plugins.stage.manager import (
    USER_STAGE_PREFIX,
    StageManager,
    StagePathParts,
    UserStagePathParts,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.connector.cursor import SnowflakeCursor


class GitStagePathParts(StagePathParts):
    def __init__(self, stage_path: str):
        self.stage = GitManager.get_stage_from_path(stage_path)
        stage_path_parts = Path(stage_path).parts
        git_repo_name = stage_path_parts[0].split(".")[-1]
        if git_repo_name.startswith("@"):
            git_repo_name = git_repo_name[1:]
        self.stage_name = "/".join([git_repo_name, *stage_path_parts[1:3], ""])
        self.directory = "/".join(stage_path_parts[3:])
        self.is_directory = True if stage_path.endswith("/") else False

    @property
    def path(self) -> str:
        return f"{self.stage_name.rstrip('/')}/{self.directory}"

    @classmethod
    def get_directory(cls, stage_path: str) -> str:
        return "/".join(Path(stage_path).parts[3:])

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
        return list(Path(file_path).parts[3 + stage_path_length : -1])


class GitManager(StageManager):
    def show_branches(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show git branches like '{like}' in {repo_name}")

    def show_tags(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show git tags like '{like}' in {repo_name}")

    def fetch(self, fqn: FQN) -> SnowflakeCursor:
        return self._execute_query(f"alter git repository {fqn} fetch")

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
        return self._execute_query(query)

    @staticmethod
    def get_stage_from_path(path: str):
        """
        Returns stage name from potential path on stage. For example
        repo/branches/main/foo/bar -> repo/branches/main/
        """
        return f"{'/'.join(Path(path).parts[0:3])}/"

    @staticmethod
    def _stage_path_part_factory(stage_path: str) -> StagePathParts:
        stage_path = StageManager.get_standard_stage_prefix(stage_path)
        if stage_path.startswith(USER_STAGE_PREFIX):
            return UserStagePathParts(stage_path)
        return GitStagePathParts(stage_path)
