from urllib.parse import urlparse

from click import ClickException
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.project.util import (
    is_valid_unquoted_identifier,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.spcs.common import handle_object_already_exists
from snowflake.connector.errors import ProgrammingError


class ImageRepositoryManager(SqlExecutionMixin):
    def get_database(self):
        return self._conn.database

    def get_schema(self):
        return self._conn.schema

    def get_role(self):
        return self._conn.role

    def get_repository_url(self, repo_name: str, with_scheme: bool = True):
        if not is_valid_unquoted_identifier(repo_name):
            raise ValueError(
                f"repo_name '{repo_name}' is not a valid unquoted Snowflake identifier"
            )
        # we explicitly do not allow this function to be used without connection database and schema set
        repo_row = self.show_specific_object(
            "image repositories", repo_name, check_schema=True
        )
        if repo_row is None:
            raise ClickException(
                f"Image repository '{repo_name}' does not exist in database '{self.get_database()}' and schema '{self.get_schema()}' or not authorized."
            )
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

    def create(self, name: str):
        try:
            return self._execute_schema_query(f"create image repository {name}")
        except ProgrammingError as e:
            handle_object_already_exists(e, ObjectType.IMAGE_REPOSITORY, name)
