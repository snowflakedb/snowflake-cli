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

import logging
from textwrap import dedent
from typing import Dict

log = logging.getLogger(__name__)

# Feature flags
IS_PERSONAL_DB_SUPPORTED = True


def _generate_snowflake_yml(
    app_id: str,
    resolved: Dict[str, str],
) -> str:
    """Generate snowflake.yml content from pre-resolved configuration values.

    All required keys (``database``, ``schema``, ``warehouse``,
    ``build_compute_pool``, ``service_compute_pool``, ``build_eai``) must
    be present and non-empty in *resolved*.  The artifact repository is
    omitted from the generated YAML; the CLI will default to
    ``<app-id>_REPO`` at deploy time.
    """

    if resolved.get("image_repository"):
        log.warning(
            "image_repository is configured but is no longer included in "
            "generated snowflake.yml. The CLI defaults to <app-id>_REPO at "
            "deploy time. You can remove the image_repository setting."
        )

    database = resolved["database"]
    schema = resolved["schema"]
    warehouse = resolved["warehouse"]
    build_compute_pool = resolved["build_compute_pool"]
    service_compute_pool = resolved["service_compute_pool"]
    build_eai = resolved["build_eai"]

    code_stage = f"{app_id.upper()}_CODE"

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
              name: {build_compute_pool}
            service_compute_pool:
              name: {service_compute_pool}
            build_eai:
              name: {build_eai}
            code_stage:
              name: {code_stage}
        """
    )
