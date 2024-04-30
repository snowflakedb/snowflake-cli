from pathlib import Path
from textwrap import dedent

from snowflake.cli.plugins.stage.manager import StageManager, StagePathParts
from snowflake.connector.cursor import SnowflakeCursor


class GitManager(StageManager):
    def show_branches(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show git branches like '{like}' in {repo_name}")

    def show_tags(self, repo_name: str, like: str) -> SnowflakeCursor:
        return self._execute_query(f"show git tags like '{like}' in {repo_name}")

    def fetch(self, repo_name: str) -> SnowflakeCursor:
        return self._execute_query(f"alter git repository {repo_name} fetch")

    def create(
        self, repo_name: str, api_integration: str, url: str, secret: str
    ) -> SnowflakeCursor:
        query = dedent(
            f"""
            create git repository {repo_name}
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

    def _split_stage_path(self, stage_path: str) -> StagePathParts:
        """
        Splits Git repository path `@repo/branch/main/dir`
            stage -> @repo/branch/main/
            stage_name -> repo/branch/main/
            directory -> dir
        For Git repository with fully qualified name `@db.schema.repo/branch/main/dir`
            stage -> @db.schema.repo/branch/main/
            stage_name -> repo/branch/main/
            directory -> dir
        """
        stage = self.get_stage_from_path(stage_path)
        stage_path_parts = Path(stage_path).parts
        git_repo_name = stage_path_parts[0].split(".")[-1]
        if git_repo_name.startswith("@"):
            git_repo_name = git_repo_name[1:]
        stage_name = "/".join([git_repo_name, *stage_path_parts[1:3], ""])
        directory = "/".join(stage_path_parts[3:])
        return StagePathParts(stage, stage_name, directory)
