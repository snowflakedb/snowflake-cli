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
from typing import Dict, Optional, cast

from snowflake.cli._plugins.apps.manager import DEFAULT_PERSONAL_WORKSPACE_NAME


def _yaml_str(v: str) -> str:
    # YAML treats bare double quotes as string delimiters and strips them on
    # round-trip, turning '"lower_db"' into 'lower_db' (then uppercased by
    # Snowflake).  Wrapping in YAML single quotes preserves embedded double
    # quotes as literal data.  Single quotes inside the value are escaped by
    # doubling them, per the YAML 1.1 single-quoted scalar spec.
    if '"' in v:
        return "'" + v.replace("'", "''") + "'"
    return v


def _generate_snowflake_yml(
    app_id: str,
    resolved: Dict[str, Optional[str]],
    *,
    use_workspace: bool,
) -> str:
    """Generate snowflake.yml content from pre-resolved configuration values.

    Required keys: ``database``, ``schema``, ``warehouse``.

    Optional keys: ``build_eai``. When omitted or ``None`` the corresponding
    block is left out of the generated YAML. ``build_eai`` is omitted when no
    external access integration is required by the builder service. The setup
    flow does not emit ``service_eai``; deploy continues to use ``build_eai``
    for the application service unless a project adds ``service_eai`` manually.

    Compute pools are never written: app services always run on
    server-managed compute pools, so ``snow app setup`` does not configure
    ``build_compute_pool`` / ``service_compute_pool``. Existing projects that
    set those fields by hand continue to work at deploy time.

    The artifact repository is omitted from the generated YAML; the CLI
    will default to ``<app-id>_REPO`` at deploy time.

    When ``use_workspace`` is true (database resolved from the user's
    personal database during ``snow app setup``), the generator emits
    ``code_workspace`` as a fully-qualified identifier pointing at a shared
    ``SNOWFLAKE_APPS`` workspace. Each app is uploaded into its own
    subdirectory at deploy time, so a single workspace serves every app the
    user owns.

    Otherwise the generator emits ``code_stage`` as a bare stage name
    resolved against the app's database and schema at deploy time.
    """

    database = cast(str, resolved["database"])
    schema = cast(str, resolved["schema"])
    warehouse = cast(str, resolved["warehouse"])
    build_eai = resolved.get("build_eai")

    if use_workspace:
        # Shared workspace: all of the user's apps live as subdirectories
        # under a single ``SNOWFLAKE_APPS`` workspace in their personal DB.
        # Fully-qualified so it does not implicitly depend on the resolved
        # database/schema.
        code_storage_block = (
            f"\n            code_workspace: "
            f"{_yaml_str(f'{database}.{schema}.{DEFAULT_PERSONAL_WORKSPACE_NAME}')}\n"
        )
    else:
        code_storage_block = f"\n            code_stage: {app_id.upper()}_CODE\n"

    build_eai_block = (
        f"\n            build_eai:\n              name: {_yaml_str(build_eai)}"
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
              database: {_yaml_str(database)}
              schema: {_yaml_str(schema)}
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

            query_warehouse: {_yaml_str(warehouse)}"""
        + build_eai_block
        + code_storage_block
    )
    return dedent(raw)
