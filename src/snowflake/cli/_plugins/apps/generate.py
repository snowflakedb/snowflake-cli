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
from typing import Dict, Optional

log = logging.getLogger(__name__)


def _generate_snowflake_yml(
    app_id: str,
    resolved: Dict[str, Optional[str]],
) -> str:
    """Generate snowflake.yml content from pre-resolved configuration values.

    Required keys: ``database``, ``schema``, ``warehouse``,
    ``build_compute_pool``, ``service_compute_pool``.

    Optional keys: ``build_eai``.  When not provided (``None``) the
    ``build_eai`` block is omitted from the generated YAML — the builder
    service will run without an external access integration.

    The artifact repository is omitted from the generated YAML; the CLI
    will default to ``<app-id>_REPO`` at deploy time.
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
    build_eai = resolved.get("build_eai")

    # code_workspace is emitted as an identifier (``DB.SCHEMA.WORKSPACE``) so
    # it is self-contained and does not implicitly depend on the app's
    # database and schema at deploy time.
    code_workspace_name = f"{app_id.upper()}_CODE"
    code_workspace_identifier = f"{database}.{schema}.{code_workspace_name}"

    build_eai_block = (
        f"\n            build_eai:\n              name: {build_eai}"
        if build_eai
        else ""
    )

    raw = (
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
              name: {service_compute_pool}"""
        + build_eai_block
        + f"\n            code_workspace: {code_workspace_identifier}\n"
    )
    return dedent(raw)
