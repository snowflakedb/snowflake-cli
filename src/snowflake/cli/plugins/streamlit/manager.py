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
import os
from pathlib import Path
from typing import List, Optional

from snowflake.cli.api.commands.experimental_behaviour import (
    experimental_behaviour_enabled,
)
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.plugins.connection.util import (
    MissingConnectionAccountError,
    MissingConnectionRegionError,
    make_snowsight_url,
)
from snowflake.cli.plugins.stage.manager import StageManager
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
        main_file: Path,
        environment_file: Optional[Path],
        pages_dir: Optional[Path],
        additional_source_files: Optional[List[Path]],
    ):
        stage_manager = StageManager()

        stage_manager.put(main_file, root_location, 4, True)

        if environment_file and environment_file.exists():
            stage_manager.put(environment_file, root_location, 4, True)

        if pages_dir and pages_dir.exists():
            stage_manager.put(pages_dir / "*.py", f"{root_location}/pages", 4, True)

        if additional_source_files:
            for file in additional_source_files:
                if os.sep in str(file):
                    destination = f"{root_location}/{str(file.parent)}"
                else:
                    destination = root_location
                stage_manager.put(file, destination, 4, True)

    def _create_streamlit(
        self,
        streamlit_id: FQN,
        main_file: Path,
        replace: Optional[bool] = None,
        experimental: Optional[bool] = None,
        query_warehouse: Optional[str] = None,
        from_stage_name: Optional[str] = None,
        title: Optional[str] = None,
    ):
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

        query.append(f"MAIN_FILE = '{main_file.name}'")

        if query_warehouse:
            query.append(f"QUERY_WAREHOUSE = {query_warehouse}")
        if title:
            query.append(f"TITLE = '{title}'")

        self._execute_query("\n".join(query))

    def deploy(
        self,
        streamlit_id: FQN,
        main_file: Path,
        environment_file: Optional[Path] = None,
        pages_dir: Optional[Path] = None,
        stage_name: Optional[str] = None,
        query_warehouse: Optional[str] = None,
        replace: Optional[bool] = False,
        additional_source_files: Optional[List[Path]] = None,
        title: Optional[str] = None,
        **options,
    ):
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
                streamlit_id,
                main_file,
                replace=replace,
                query_warehouse=query_warehouse,
                experimental=True,
                title=title,
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
                main_file,
                environment_file,
                pages_dir,
                additional_source_files,
            )
        else:
            """
            1. Create stage
            2. Upload files to created stage
            3. Create streamlit from stage
            """
            stage_manager = StageManager()

            stage_name = stage_name or "streamlit"
            stage_name = FQN.from_string(stage_name).using_connection(self._conn)

            stage_manager.create(stage_name=stage_name)

            root_location = stage_manager.get_standard_stage_prefix(
                f"{stage_name}/{streamlit_name_for_root_location}"
            )

            self._put_streamlit_files(
                root_location,
                main_file,
                environment_file,
                pages_dir,
                additional_source_files,
            )

            self._create_streamlit(
                streamlit_id,
                main_file,
                replace=replace,
                query_warehouse=query_warehouse,
                from_stage_name=root_location,
                experimental=False,
                title=title,
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
