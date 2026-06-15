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

"""Integration test for the ``snow app deploy`` code-upload phase.

Scope: this module exercises *only* the workspace code-upload path via
``snow app deploy --upload-only``. That is the code path behind the
Windows ``file://`` URI fix — each bundled file is sent to the workspace
live version with a ``PUT file://...``. ``--upload-only`` stops before the
build and service phases, so the test needs no compute pool or other
container-service resources and runs cheaply against a real account.

The full deploy flow (build + service) is covered by
``tests_integration/tests_using_container_services/spcs/test_snowflake_apps.py``;
do not duplicate it here.
"""

from __future__ import annotations

import os
import textwrap
import uuid
from pathlib import Path

import pytest

DATABASE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_DATABASE", "SNOWCLI_DB")
SCHEMA = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_SCHEMA", "public")
WAREHOUSE = os.environ.get("SNOWFLAKE_CONNECTIONS_INTEGRATION_WAREHOUSE", "xsmall")


@pytest.fixture()
def unique_workspace(snowflake_session):
    """Yield a unique workspace name and drop it on teardown.

    The upload path creates the workspace if it does not exist, so the test
    only needs to guarantee a non-colliding name and clean it up afterwards.
    """
    ws_name = f"SNOW_APP_WS_TEST_{uuid.uuid4().hex[:8]}"
    yield ws_name
    try:
        snowflake_session.execute_string(
            f"DROP WORKSPACE IF EXISTS {DATABASE}.{SCHEMA}.{ws_name}"
        )
    except Exception:
        pass  # best-effort cleanup


@pytest.fixture()
def unique_stage(snowflake_session):
    """Yield a unique stage name and drop it on teardown.

    The stage upload path creates the stage if it does not exist, so the test
    only needs to guarantee a non-colliding name and clean it up afterwards.
    """
    stage_name = f"SNOW_APP_STAGE_TEST_{uuid.uuid4().hex[:8]}"
    yield stage_name
    try:
        snowflake_session.execute_string(
            f"DROP STAGE IF EXISTS {DATABASE}.{SCHEMA}.{stage_name}"
        )
    except Exception:
        pass  # best-effort cleanup


@pytest.mark.integration
def test_deploy_upload_only_uploads_code_to_workspace(
    runner,
    temporary_working_directory,
    unique_workspace,
):
    """``snow app deploy --upload-only`` uploads bundled files to a workspace.

    Verifies the workspace ``PUT file://...`` upload (including a nested file)
    succeeds against a real account, guarding the local-file-URI construction
    used by ``upload_to_workspace`` from regressions.
    """
    ws_name = unique_workspace
    app_name = f"WS_UPLOAD_APP_{uuid.uuid4().hex[:8]}"

    project_dir = Path(temporary_working_directory)
    (project_dir / "app" / "nested").mkdir(parents=True)
    (project_dir / "app" / "main.py").write_text("print('hello from snowflake app')\n")
    (project_dir / "app" / "nested" / "util.py").write_text("X = 1\n")

    (project_dir / "snowflake.yml").write_text(
        textwrap.dedent(
            f"""\
            definition_version: "2"
            entities:
              ws_app:
                type: snowflake-app
                identifier:
                  name: {app_name}
                  database: {DATABASE}
                  schema: {SCHEMA}
                artifacts:
                  - src: app/*
                    dest: ./
                query_warehouse: {WAREHOUSE}
                code_workspace:
                  name: {ws_name}
            """
        )
    )

    result = runner.invoke_with_connection(
        ["app", "deploy", "--entity-id", "ws_app", "--upload-only"]
    )
    assert result.exit_code == 0, f"Upload failed:\n{result.output}"

    # The upload-only path reports the workspace destination on success, and
    # prints one "Uploaded ..." line per file as each PUT completes — so both
    # the top-level file and the nested file must appear.
    assert "Artifacts uploaded to" in result.output
    assert ws_name in result.output
    assert "main.py" in result.output
    assert os.path.join("nested", "util.py") in result.output


@pytest.mark.integration
def test_deploy_upload_only_uploads_code_to_stage(
    runner,
    temporary_working_directory,
    unique_stage,
):
    """``snow app deploy --upload-only`` uploads bundled files to a stage.

    Verifies the stage ``PUT file://...`` upload (including a nested file)
    succeeds against a real account. A bundle with subdirectories is the case
    that previously failed with connector error 253006 (``Not a file but a
    directory``) when the stage upload globbed ``PUT <dir>/*``; this guards the
    file-by-file ``upload_to_stage`` path from that regression.
    """
    stage_name = unique_stage
    app_name = f"STAGE_UPLOAD_APP_{uuid.uuid4().hex[:8]}"

    project_dir = Path(temporary_working_directory)
    (project_dir / "app" / "nested").mkdir(parents=True)
    (project_dir / "app" / "main.py").write_text("print('hello from snowflake app')\n")
    (project_dir / "app" / "nested" / "util.py").write_text("X = 1\n")

    (project_dir / "snowflake.yml").write_text(
        textwrap.dedent(
            f"""\
            definition_version: "2"
            entities:
              stage_app:
                type: snowflake-app
                identifier:
                  name: {app_name}
                  database: {DATABASE}
                  schema: {SCHEMA}
                artifacts:
                  - src: app/*
                    dest: ./
                query_warehouse: {WAREHOUSE}
                code_stage:
                  name: {stage_name}
            """
        )
    )

    result = runner.invoke_with_connection(
        ["app", "deploy", "--entity-id", "stage_app", "--upload-only"]
    )
    assert result.exit_code == 0, f"Upload failed:\n{result.output}"

    # The upload-only path reports the stage destination on success, and prints
    # one "Uploaded ..." line per file as each PUT completes — so both the
    # top-level file and the nested file must appear.
    assert "Artifacts uploaded to" in result.output
    assert stage_name in result.output
    assert "main.py" in result.output
    assert os.path.join("nested", "util.py") in result.output
