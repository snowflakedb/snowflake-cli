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
from typing import Dict

# Feature flags
IS_PERSONAL_DB_SUPPORTED = False  # Will be enabled in the future

DEFAULT_SCHEMA = "SNOW_APPS"


def _generate_snowflake_yml(
    app_id: str,
    resolved: Dict[str, str],
) -> str:
    """Generate snowflake.yml content from pre-resolved configuration values.

    All required keys (``database``, ``schema``, ``warehouse``,
    ``compute_pool``, ``build_eai``) must be present and non-empty in
    *resolved*.  The optional key ``image_repository`` is included only
    when provided.
    """

    database = resolved["database"]
    schema = resolved["schema"]
    warehouse = resolved["warehouse"]
    compute_pool = resolved["compute_pool"]
    build_eai = resolved["build_eai"]
    image_repository = resolved.get("image_repository")

    code_stage = f"{app_id.upper()}_CODE"

    repo_lines = ""
    if image_repository:
        repo_lines = (
            f"\n            image_repository:\n              name: {image_repository}"
        )

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
            artifacts:
              - src: ./*
                dest: ./
                ignore:
                  - node_modules
                  - .env*
                  - __pycache__
                  - "*.pyc"
                  - .next
                  - .git
                  - snowflake.log

            query_warehouse: {warehouse}
            build_compute_pool:
              name: {compute_pool}
            service_compute_pool:
              name: {compute_pool}
            build_eai:
              name: {build_eai}{repo_lines}
            code_stage:
              name: {code_stage}
        """
    )
