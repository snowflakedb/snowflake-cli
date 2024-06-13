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

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import make_snowsight_url
from snowflake.cli.plugins.notebook.exceptions import NotebookStagePathError
from snowflake.cli.plugins.notebook.types import NotebookName, NotebookStagePath


class NotebookManager(SqlExecutionMixin):
    def execute(self, notebook_name: NotebookName):
        query = f"EXECUTE NOTEBOOK {notebook_name}()"
        return self._execute_query(query=query)

    def get_url(self, notebook_name: NotebookName):
        fqn = FQN.from_string(notebook_name).using_connection(self._conn)
        return make_snowsight_url(
            self._conn,
            f"/#/notebooks/{fqn.url_identifier}",
        )

    @staticmethod
    def parse_stage_as_path(notebook_file: NotebookName) -> Path:
        """Parses notebook file path to pathlib.Path."""
        if not notebook_file.endswith(".ipynb"):
            raise NotebookStagePathError(notebook_file)
        stage_path = Path(notebook_file)
        if len(stage_path.parts) < 2:
            raise NotebookStagePathError(notebook_file)

        return stage_path

    def create(
        self,
        notebook_name: NotebookName,
        notebook_file: NotebookStagePath,
    ) -> str:
        notebook_fqn = FQN.from_string(notebook_name).using_connection(self._conn)
        stage_path = self.parse_stage_as_path(notebook_file)

        queries = dedent(
            f"""
            CREATE OR REPLACE NOTEBOOK {notebook_fqn.identifier}
            FROM '{stage_path.parent}'
            QUERY_WAREHOUSE = '{cli_context.connection.warehouse}'
            MAIN_FILE = '{stage_path.name}';

            ALTER NOTEBOOK {notebook_fqn.identifier} ADD LIVE VERSION FROM LAST;
            """
        )
        self._execute_queries(queries=queries)

        return make_snowsight_url(
            self._conn, f"/#/notebooks/{notebook_fqn.url_identifier}"
        )
