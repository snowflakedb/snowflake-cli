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
from typing import List, Optional

from click import ClickException
from snowflake.cli._plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
    make_snowsight_url,
)
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
)
from snowflake.cli._plugins.streamlit.streamlit_project_paths import (
    StreamlitProjectPaths,
)
from snowflake.cli.api.artifacts.upload import sync_artifacts_with_stage
from snowflake.cli.api.commands.experimental_behaviour import (
    experimental_behaviour_enabled,
)
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError

log = logging.getLogger(__name__)


class StreamlitManager(SqlExecutionMixin):
    def execute(self, app_name: FQN):
        query = f"EXECUTE STREAMLIT {app_name.sql_identifier}()"
        return self.execute_query(query=query)

    def share(self, streamlit_name: FQN, to_role: str) -> SnowflakeCursor:
        return self.execute_query(
            f"grant usage on streamlit {streamlit_name.sql_identifier} to role {to_role}"
        )

    def _upload_artifacts(
        self,
        streamlit_project_paths: StreamlitProjectPaths,
        stage_root: str,
        prune: bool,
        artifacts: Optional[List[PathMapping]] = None,
    ):
        with cli_console.phase(f"Deploying files to {stage_root}"):
            sync_artifacts_with_stage(
                project_paths=streamlit_project_paths,
                stage_root=stage_root,
                prune=prune,
                artifacts=artifacts,
            )

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
        if streamlit.imports:
            query.append(streamlit.get_imports_sql())

        if not streamlit.query_warehouse:
            cli_console.warning(
                "[Deprecation] In next major version we will remove default query_warehouse='streamlit'."
            )
            query.append(f"QUERY_WAREHOUSE = 'streamlit'")
        else:
            query.append(f"QUERY_WAREHOUSE = {streamlit.query_warehouse}")

        if streamlit.title:
            query.append(f"TITLE = '{streamlit.title}'")

        if streamlit.comment:
            query.append(f"COMMENT = '{streamlit.comment}'")

        if streamlit.external_access_integrations:
            query.append(streamlit.get_external_access_integrations_sql())

        if streamlit.secrets:
            query.append(streamlit.get_secrets_sql())

        self.execute_query("\n".join(query))

    def deploy(
        self,
        streamlit: StreamlitEntityModel,
        streamlit_project_paths: StreamlitProjectPaths,
        replace: bool = False,
        prune: bool = False,
    ):
        streamlit_id = streamlit.fqn.using_connection(self._conn)
        if (
            ObjectManager().object_exists(object_type="streamlit", fqn=streamlit_id)
            and not replace
        ):
            raise ClickException(
                f"Streamlit {streamlit.fqn} already exist. If you want to replace it use --replace flag."
            )

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
                    self.execute_query(
                        f"ALTER STREAMLIT {streamlit_id.identifier} ADD LIVE VERSION FROM LAST"
                    )
                elif not FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled():
                    self.execute_query(
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
                stage_root = f"{embedded_stage_name}/versions/live"
            else:
                stage_root = f"{embedded_stage_name}/default_checkout"

            self._upload_artifacts(
                streamlit_project_paths,
                stage_root,
                prune=prune,
                artifacts=streamlit.artifacts,
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

            stage_root = stage_manager.get_standard_stage_prefix(
                f"{stage_name}/{streamlit_name_for_root_location}"
            )

            self._upload_artifacts(
                streamlit_project_paths,
                stage_root,
                prune=prune,
                artifacts=streamlit.artifacts,
            )

            self._create_streamlit(
                streamlit=streamlit,
                replace=replace,
                from_stage_name=stage_root,
                experimental=False,
            )

            self.grant_privileges(streamlit)

        return self.get_url(streamlit_name=streamlit_id)

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
