from typing import Dict
from urllib.parse import urlparse

from click import ClickException
from snowflake.cli.api.project.util import (
    escape_like_pattern,
    is_valid_unquoted_identifier,
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
        if not is_valid_unquoted_identifier(repo_name):
            raise ValueError(
                f"repo_name '{repo_name}' is not a valid unquoted Snowflake identifier"
            )

        repo_name = repo_name.upper()

        # because image repositories only support unquoted identifiers, SHOW LIKE should only return one or zero rows
        repository_list_query = (
            f"show image repositories like '{escape_like_pattern(repo_name)}'"
        )

        result_set = self._execute_schema_query(
            repository_list_query, cursor_class=DictCursor
        )
        results = result_set.fetchall()

        if len(results) == 0:
            raise ClickException(
                f"Image repository '{repo_name}' does not exist in database '{self.get_database()}' and schema '{self.get_schema()}' or not authorized."
            )
        elif len(results) > 1:
            raise ClickException(
                f"Found more than one image repository with name matching '{repo_name}'. This is unexpected."
            )
        return results[0]

    def get_repository_url(self, repo_name: str, with_scheme: bool = True):
        if not is_valid_unquoted_identifier(repo_name):
            raise ValueError(
                f"repo_name '{repo_name}' is not a valid unquoted Snowflake identifier"
            )
        repo_row = self.get_repository_row(repo_name)
        if with_scheme:
            return f"https://{repo_row['repository_url']}"
        else:
            return repo_row["repository_url"]

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
