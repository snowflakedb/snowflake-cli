# Copyright (c) 2026 Snowflake Inc.
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

from textwrap import dedent
from typing import Optional

from snowflake.cli._plugins.apps.manager import (
    _get_compute_pool,
    _get_external_access,
)
from snowflake.cli.api.project.util import get_env_username

# Feature flags
IS_PERSONAL_DB_SUPPORTED = False  # Will be enabled in the future

DEFAULT_ARTIFACT_REPOSITORY = "SNOW_APPS_DEFAULT_ARTIFACT_REPOSITORY"
DEFAULT_IMAGE_REPOSITORY = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"


def _generate_snowflake_yml(
    app_id: str,
    warehouse: Optional[str],
    database: Optional[str] = None,
) -> str:
    """Generate snowflake.yml content for a Snow App project."""

    username = get_env_username().upper()

    # Database: use personal DB if supported, otherwise use connection database
    if IS_PERSONAL_DB_SUPPORTED:
        database = f"USER${username}"
    else:
        database = database or "<% ctx.connection.database %>"

    # Schema: SNOW_APP_<APP_ID>_<USERNAME>
    schema = f"SNOW_APP_{app_id.upper()}_{username}"

    # Stage: <APP_ID>_CODE
    code_stage = f"{app_id.upper()}_CODE"

    # Compute pool: check for existing pools
    compute_pool = _get_compute_pool()
    if compute_pool:
        compute_pool_yaml = f"""build_compute_pool:
              name: {compute_pool}
            service_compute_pool:
              name: {compute_pool}"""
    else:
        compute_pool_yaml = f"""build_compute_pool:
              name: null
            service_compute_pool:
              name: null"""

    # Build EAI: check for existing integrations
    build_eai = _get_external_access(app_id)
    if build_eai:
        build_eai_yaml = f"""build_eai:
              name: {build_eai}"""
    else:
        build_eai_yaml = "build_eai: null"

    # TODO: Check if artifact repository exists
    artifact_repository = DEFAULT_ARTIFACT_REPOSITORY

    return dedent(
        f"""\
        definition_version: "2"

        entities:
          {app_id}:
            type: snowflake-app
            identifier:
              name: {app_id.upper()}
              database: {database}
              schema: {schema}
            meta:
              title: {app_id}
              description: null
              icon: null
            artifacts:
              - src: app/*
                dest: ./
                ignore:
                  - node_modules
                  - .env
                  - __pycache__
                  - "*.pyc"
                  - .next
                  - .git

            query_warehouse: {warehouse or "<% ctx.connection.warehouse %>"}
            {compute_pool_yaml}
            {build_eai_yaml}
            service_eai: null
            artifact_repository:
              name: {artifact_repository}
            image_repository:
              name: {DEFAULT_IMAGE_REPOSITORY}
            code_stage:
              name: {code_stage}
        """
    )
