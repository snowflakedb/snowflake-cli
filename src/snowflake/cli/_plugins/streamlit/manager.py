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
from pathlib import Path
from typing import List, Optional

from snowflake.cli._plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
    make_snowsight_url,
)
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.commands.experimental_behaviour import (
    experimental_behaviour_enabled,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

log = logging.getLogger(__name__)


class StreamlitManager(SqlExecutionMixin):
    def share(self, streamlit_name: FQN, to_role: str) -> SnowflakeCursor:
        return self._execute_query(
            f"grant usage on streamlit {streamlit_name.sql_identifier} to role {to_role}"
        )

    def _put_streamlit_files(
        self,
        root_location: str,
        artifacts: Optional[List[Path]] = None,
    ):
        cli_console.step(f"Deploying files to {root_location}")
        if not artifacts:
            return
        stage_manager = StageManager()
        for file in artifacts:
            if file.is_dir():
                if not any(file.iterdir()):
                    cli_console.warning(f"Skipping empty directory: {file}")
                    continue

                stage_manager.put(
                    f"{file.joinpath('*')}", f"{root_location}/{file}", 4, True
                )
            elif len(file.parts) > 1:
                stage_manager.put(file, f"{root_location}/{file.parent}", 4, True)
            else:
                stage_manager.put(file, root_location, 4, True)

    def _create_streamlit(
        self,
        streamlit: StreamlitEntityModel,
        replace: Optional[bool] = None,
        experimental: Optional[bool] = None,
        from_stage_name: Optional[str] = None,
    ):
        streamlit_id = streamlit.fqn.using_connection(self._conn)
        cli_console.step(f"Creating {streamlit_id} Streamlit")
        query = []
        if replace:
            query.append(f"CREATE OR REPLACE STREAMLIT {streamlit_id.sql_identifier}")
        elif experimental:
            # For experimental behaviour, we need to use CREATE STREAMLIT IF NOT EXISTS
            # for a streamlit app with an embedded stage
            # because this is analogous to the behavior for non-experimental
            # deploy which does CREATE STAGE IF NOT EXISTS
            query.append(
                f"CREATE STREAMLIT IF NOT EXISTS {streamlit_id.sql_identifier}"
            )
        else:
            query.append(f"CREATE STREAMLIT {streamlit_id.sql_identifier}")

        if from_stage_name:
            query.append(f"ROOT_LOCATION = '{from_stage_name}'")

        query.append(f"MAIN_FILE = '{streamlit.main_file}'")

        if streamlit.query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {streamlit.query_warehouse}")
        if streamlit.title:
            query.append(f"TITLE = '{streamlit.title}'")

        if streamlit.external_access_integrations:
            query.append(streamlit.get_external_access_integrations_sql())

        if streamlit.secrets:
            query.append(streamlit.get_secrets_sql())

        self._execute_query("\n".join(query))

    def deploy(self, streamlit: StreamlitEntityModel, replace: bool = False):
        streamlit_id = streamlit.fqn.using_connection(self._conn)

        # for backwards compatibility - quoted stage path might be case-sensitive
        # https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers
        streamlit_name_for_root_location = streamlit_id.name
        use_versioned_stage = FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled()
        if (
            experimental_behaviour_enabled()
            or FeatureFlag.ENABLE_STREAMLIT_EMBEDDED_STAGE.is_enabled()
            or use_versioned_stage
        ):
            """
            1. Create streamlit object
            2. Upload files to embedded stage
            """
            # TODO: Support from_stage
            # from_stage_stmt = f"FROM_STAGE = '{stage_name}'" if stage_name else ""
            self._create_streamlit(
                streamlit=streamlit,
                replace=replace,
                experimental=True,
            )
            try:
                if use_versioned_stage:
                    self._execute_query(
                        f"ALTER STREAMLIT {streamlit_id.identifier} ADD LIVE VERSION FROM LAST"
                    )
                elif not FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled():
                    self._execute_query(
                        f"ALTER streamlit {streamlit_id.identifier} CHECKOUT"
                    )
            except ProgrammingError as e:
                # If an error is raised because a CHECKOUT has already occurred or a LIVE VERSION already exists, simply skip it and continue
                if "Checkout already exists" in str(
                    e
                ) or "There is already a live version" in str(e):
                    log.info("Checkout already exists, continuing")
                else:
                    raise

            stage_path = streamlit_id.identifier
            embedded_stage_name = f"snow://streamlit/{stage_path}"
            if use_versioned_stage:
                # "LIVE" is the only supported version for now, but this may change later.
                root_location = f"{embedded_stage_name}/versions/live"
            else:
                root_location = f"{embedded_stage_name}/default_checkout"

            self._put_streamlit_files(
                root_location,
                streamlit.artifacts,
            )
        else:
            """
            1. Create stage
            2. Upload files to created stage
            3. Create streamlit from stage
            """
            stage_manager = StageManager()

            stage_name = streamlit.stage or "streamlit"
            stage_name = FQN.from_string(stage_name).using_connection(self._conn)

            cli_console.step(f"Creating {stage_name} stage")
            stage_manager.create(fqn=stage_name)

            root_location = stage_manager.get_standard_stage_prefix(
                f"{stage_name}/{streamlit_name_for_root_location}"
            )

            self._put_streamlit_files(root_location, streamlit.artifacts)

            self._create_streamlit(
                streamlit=streamlit,
                replace=replace,
                from_stage_name=root_location,
                experimental=False,
            )

        return self.get_url(streamlit_name=streamlit_id)

    def get_url(self, streamlit_name: FQN) -> str:
        try:
            fqn = streamlit_name.using_connection(self._conn)
            return make_snowsight_url(
                self._conn,
                f"/#/streamlit-apps/{fqn.url_identifier}",
            )
        except (MissingConnectionRegionError, MissingConnectionAccountError) as e:
            return "https://app.snowflake.com"
