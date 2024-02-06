from typing import Dict
from urllib.parse import urlparse

import click
from click import ClickException
from snowflake.cli.api.project.util import (
    escape_like_pattern,
    is_valid_identifier,
    unquote_identifier,
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

        # Unquoted identifiers are resolved as all upper case in Snowflake while quoted identifiers are case-sensitive
        repo_name = unquote_identifier(repo_name)

        repository_list_query = (
            f"show image repositories like '{escape_like_pattern(repo_name)}'"
        )
        result_set = self._execute_schema_query(
            repository_list_query, cursor_class=DictCursor
        )
        results = result_set.fetchall()

        # because SHOW LIKE uses case-insensitive matching, results may return multiple rows
        results = [r for r in results if r["name"] == repo_name]

        colored_repo_name = click.style(f"'{repo_name}'", fg="green")
        if len(results) == 0:
            raise ClickException(
                f"Image repository {colored_repo_name} does not exist or not authorized."
            )
        elif len(results) > 1:
            raise ClickException(
                f"Found more than one image repository with name matching {colored_repo_name}. This is unexpected."
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

    def _remove_scheme(self, url: str) -> str:
        if not urlparse(url).scheme and not url.startswith("//"):
            url = f"//{url}"
        parsed_url = urlparse(url)
        return f"{parsed_url.netloc}{parsed_url.path}"

    def get_repository_url_strip_scheme(self, repo_name):
        return self._remove_scheme(self.get_repository_url(repo_name))
