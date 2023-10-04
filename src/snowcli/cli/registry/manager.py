import json
import requests
import sys
import base64
from urllib.parse import urlparse
from click import ClickException

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor


class RegistryManager(SqlExecutionMixin):
    def get_database(self):
        return self._conn.database

    def get_schema(self):
        return self._conn.schema

    def get_role(self):
        return self._conn.role

    def get_token(self):
        """
        Get token to authenticate with registry.
        """
        self._execute_query(
            "alter session set PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = 'json'"
        ).fetchall()
        # disable session deletion
        self._conn._all_async_queries_finished = lambda: False
        if self._conn._rest is None:
            raise Exception("Failed to connect to Snowflake to retrieve token.")
        # obtain and create the token
        token_data = self._conn._rest._token_request("ISSUE")

        return {
            "token": token_data["data"]["sessionToken"],
            "expires_in": token_data["data"]["validityInSecondsST"],
        }

    def login_to_registry(self, repo_url):
        token = json.dumps(self.get_token())
        parsed_url = urlparse(repo_url)

        scheme = parsed_url.scheme
        host = parsed_url.netloc

        login_url = f"{scheme}://{host}/login"
        creds = base64.b64encode(f"0sessiontoken:{token}".encode("utf-8"))
        creds = creds.decode("utf-8")

        resp = requests.get(login_url, headers={"Authorization": f"Basic {creds}"})

        if resp.status_code != 200:
            raise ClickException(f"Failed to login to the repository {resp.text}")

        return json.loads(resp.text)["token"]

    def get_registry_url(self, repo_name: str) -> SnowflakeCursor:
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

        result_set = self.get_registry_url(repo_name=repo_name)

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
