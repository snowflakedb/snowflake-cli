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

from snowflake.cli._plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
    make_snowsight_url,
)
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)


class StreamlitManager(SqlExecutionMixin):
    def execute(self, app_name: FQN):
        query = f"EXECUTE STREAMLIT {app_name.sql_identifier}()"
        return self.execute_query(query=query)

    def share(self, streamlit_name: FQN, to_role: str) -> SnowflakeCursor:
        return self.execute_query(
            f"grant usage on streamlit {streamlit_name.sql_identifier} to role {to_role}"
        )

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
