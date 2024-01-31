from urllib.parse import urlparse

from click import ClickException
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class ImageRepositoryManager(SqlExecutionMixin):
    def get_database(self):
        return self._conn.database

    def get_schema(self):
        return self._conn.schema

    def get_role(self):
        return self._conn.role

    def get_repository_url_list(self, repo_name: str) -> SnowflakeCursor:
        role = self.get_role()
        database = self.get_database()
        schema = self.get_schema()

        registry_query = f"""
            use role {role};
            use database {database};
            use schema {schema};
            show image repositories like '{repo_name}';
            """

        return self._execute_query(registry_query)

    def get_repository_url(self, repo_name):
        database = self.get_database()
        schema = self.get_schema()

        result_set = self.get_repository_url_list(repo_name=repo_name)

        results = result_set.fetchall()
        if len(results) == 0:
            raise ClickException(
                f"Specified repository name {repo_name} not found in database {database} and schema {schema}"
            )
        else:
            if len(results) > 1:
                raise Exception(
                    f"Found more than one repositories with name {repo_name}. This is unexpected."
                )

        return f"https://{results[0][4]}"

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
