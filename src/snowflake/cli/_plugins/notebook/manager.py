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

from pathlib import Path
from textwrap import dedent

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.notebook.exceptions import NotebookFilePathError
from snowflake.cli._plugins.notebook.types import NotebookStagePath
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.api.stage_path import StagePath


class NotebookManager(SqlExecutionMixin):
    def execute(self, notebook_name: FQN):
        query = f"EXECUTE NOTEBOOK {notebook_name.sql_identifier}()"
        return self.execute_query(query=query)

    def get_url(self, notebook_name: FQN):
        fqn = notebook_name.using_connection(self._conn)
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{fqn.url_identifier}",
        )

    @staticmethod
    def parse_stage_as_path(notebook_file: str) -> Path:
        """Parses notebook file path to pathlib.Path."""
        if not notebook_file.endswith(".ipynb"):
            raise NotebookFilePathError(notebook_file)
        # we don't perform any operations on the path, so we don't need to differentiate git repository paths
        stage_path = StagePath.from_stage_str(notebook_file)
        if len(stage_path.parts) < 1:
            raise NotebookFilePathError(notebook_file)

        return stage_path

    def create(
        self,
        notebook_name: FQN,
        notebook_file: NotebookStagePath,
    ) -> str:
        notebook_fqn = notebook_name.using_connection(self._conn)
        stage_path = self.parse_stage_as_path(notebook_file)

        queries = dedent(
            f"""
            CREATE OR REPLACE NOTEBOOK {notebook_fqn.sql_identifier}
            FROM '{stage_path.parent}'
            QUERY_WAREHOUSE = '{get_cli_context().connection.warehouse}'
            MAIN_FILE = '{stage_path.name}';
            // Cannot use IDENTIFIER(...)
            ALTER NOTEBOOK {notebook_fqn.identifier} ADD LIVE VERSION FROM LAST;
            """
        )
        self.execute_queries(queries=queries)

        return make_snowsight_url(
            self._conn, f"/#/notebooks/{notebook_fqn.url_identifier}"
        )
