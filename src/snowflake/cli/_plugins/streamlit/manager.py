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

import logging
from typing import List, Sequence, Tuple

from snowflake.cli._plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
    make_snowsight_url,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.cli.api.project.util import to_identifier, to_string_literal
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


def _location_identifiers(app_fqn: FQN) -> List[Tuple[str, str]]:
    """``(object type, IDENTIFIER(...))`` for the app's database and schema.

    Each part is quoted on its own before being joined, since `FQN.prefix`
    would substitute `PUBLIC` for a missing schema.
    """
    if not app_fqn.database:
        return []
    database = to_identifier(app_fqn.database)
    locations = [("DATABASE", f"IDENTIFIER({to_string_literal(database)})")]
    if app_fqn.schema:
        qualified = f"{database}.{to_identifier(app_fqn.schema)}"
        locations.append(("SCHEMA", f"IDENTIFIER({to_string_literal(qualified)})"))
    return locations


class StreamlitManager(SqlExecutionMixin):
    def execute(self, app_name: FQN):
        query = f"EXECUTE STREAMLIT {app_name.sql_identifier}()"
        return self.execute_query(query=query)

    def share(
        self,
        streamlit_name: FQN,
        grantees: Sequence[Grant],
        with_grant_option: bool = False,
    ) -> List[SnowflakeCursor]:
        """Grant USAGE on the app to each grantee, one statement each."""
        grant_option = " WITH GRANT OPTION" if with_grant_option else ""
        return [
            self.execute_query(
                f"GRANT USAGE ON STREAMLIT {streamlit_name.sql_identifier}"
                f" TO {grantee.grantee_sql}{grant_option}"
            )
            for grantee in grantees
        ]

    def grant_location_usage(
        self, app_fqn: FQN, grantees: Sequence[Grant]
    ) -> List[str]:
        """Grant USAGE on the app's database and schema, best effort.

        Returns one message per refusal. `GRANT USAGE ON STREAMLIT` does not
        depend on these grants, so the share stands and the caller warns.

        Grants are issued without checking for an existing one: `SHOW GRANTS ON
        DATABASE` lists no users at all, and does not expand role inheritance,
        so a check would report a gap that isn't there.
        """
        problems: List[str] = []
        for object_type, identifier in _location_identifiers(app_fqn):
            for grantee in grantees:
                try:
                    self.execute_query(
                        f"GRANT USAGE ON {object_type} {identifier}"
                        f" TO {grantee.grantee_sql}"
                    )
                except ProgrammingError as error:
                    problems.append(
                        f"Could not grant USAGE on {object_type.lower()}"
                        f" to {sanitize_for_terminal(grantee.grantee_sql)}:"
                        f" {sanitize_for_terminal(str(error.msg))}"
                    )
        return problems

    def grant_privileges(self, entity_model: StreamlitEntityModel):
        if not entity_model.grants:
            return
        for grant in entity_model.grants:
            self.execute_query(grant.get_grant_sql(entity_model))

    def get_url(self, streamlit_name: FQN) -> str:
        try:
            fqn = streamlit_name.using_connection(self._conn)
            return make_snowsight_url(
                self._conn,
                f"/#/streamlit-apps/{fqn.url_identifier}",
            )
        except (MissingConnectionRegionError, MissingConnectionAccountError) as e:
            return "https://app.snowflake.com"
