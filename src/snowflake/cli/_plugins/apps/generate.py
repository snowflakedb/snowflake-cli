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
from typing import Dict, Optional

from snowflake.cli._plugins.apps.manager import (
    _get_compute_pool,
    _get_external_access,
)
from snowflake.cli.api.project.util import get_env_username

# Feature flags
IS_PERSONAL_DB_SUPPORTED = False  # Will be enabled in the future

DEFAULT_SCHEMA = "SNOW_APPS"

def _generate_snowflake_yml(
    app_id: str,
    warehouse: Optional[str],
    database: Optional[str] = None,
    config_overrides: Optional[Dict[str, str]] = None,
) -> str:
    """Generate snowflake.yml content for a Snow App project.

    *config_overrides* is an optional dict of defaults read from the
    config table.  Values here act as a fallback: they are only used
    when the corresponding explicit parameter is ``None``, but they
    take priority over built-in defaults (object-existence checks).
    """

    overrides = config_overrides or {}
    username = get_env_username().upper()

    if IS_PERSONAL_DB_SUPPORTED:
        database = f"USER${username}"
    else:
        database = database or overrides.get("database")

    warehouse = warehouse or overrides.get("warehouse")
    schema = overrides.get("schema") or DEFAULT_SCHEMA

    # Stage: <APP_ID>_CODE
    code_stage = f"{app_id.upper()}_CODE"

    # Compute pool: config table > built-in object-existence check
    compute_pool = overrides.get("compute_pool") or _get_compute_pool()
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

    # Build EAI: config table > built-in object-existence check
    build_eai = overrides.get("eai") or _get_external_access(app_id)
    if build_eai:
        build_eai_yaml = f"""build_eai:
              name: {build_eai}"""
    else:
        build_eai_yaml = "build_eai: null"

    image_repository = overrides.get("image_repository")

    repo_lines = ""
    if image_repository:
        repo_lines += (
            f"\n            image_repository:\n              name: {image_repository}"
        )

    db_yaml = database if database else "null"
    wh_yaml = warehouse if warehouse else "null"

    return dedent(
        f"""\
        definition_version: "2"

        entities:
          {app_id}:
            type: snowflake-app
            identifier:
              name: {app_id.upper()}
              database: {db_yaml}
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
                  - .env*
                  - __pycache__
                  - "*.pyc"
                  - .next
                  - .git
                  - snowflake.log

            query_warehouse: {wh_yaml}
            {compute_pool_yaml}
            {build_eai_yaml}
            service_eai: null{repo_lines}
            code_stage:
              name: {code_stage}
        """
    )
