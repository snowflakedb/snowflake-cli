from typing import Dict
from urllib.parse import urlparse

from click import ClickException
from snowflake.cli.api.project.util import (
    escape_like_pattern,
    is_valid_identifier,
    is_valid_quoted_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import DictCursor


class ImageRepositoryManager(SqlExecutionMixin):
    def get_database(self):
        return self._conn.database

    def get_schema(self):
        return self._conn.schema

    def get_role(self):
        return self._conn.role

    def get_repository_row(self, repo_name: str) -> Dict:
        if not is_valid_identifier(repo_name):
            raise ValueError(
                f"repo_name '{repo_name}' is not a valid Snowflake identifier"
            )
        database = self.get_database()
        schema = self.get_schema()
        name_is_quoted = is_valid_quoted_identifier(repo_name)

        # if repo_name is a quoted identifier, strip quotes before using as pattern
        repo_name_raw = repo_name[1:-1] if name_is_quoted else repo_name.upper()
        repository_list_query = (
            f"show image repositories like '{escape_like_pattern(repo_name_raw)}'"
        )
        result_set = self._execute_query(repository_list_query, cursor_class=DictCursor)
        results = result_set.fetchall()

        # because SHOW LIKE uses case-insensitive matching, results may return multiple rows
        results = [r for r in results if r["name"] == repo_name_raw]

        if len(results) == 0:
            raise ClickException(
                f"Specified repository name {repo_name} not found in database {database} and schema {schema}"
            )
        elif len(results) > 1:
            raise Exception(
                f"Found more than one repositories with name {repo_name}. This is unexpected."
            )
        return results[0]

    def get_repository_url(self, repo_name: str):
        if not is_valid_identifier(repo_name):
            raise ValueError(
                f"repo_name '{repo_name}' is not a valid Snowflake identifier"
            )
        repo_row = self.get_repository_row(repo_name)
        return f"https://{repo_row['repository_url']}"

    def get_repository_api_url(self, repo_url):
        """
        Converts a repo URL to a registry OCI API URL.
        https://reg.com/db/schema/repo becomes https://reg.com/v2/db/schema/repo
        """
        parsed_url = urlparse(repo_url)

        scheme = parsed_url.scheme
        host = parsed_url.netloc
        path = parsed_url.path

        return f"{scheme}://{host}/v2{path}"
