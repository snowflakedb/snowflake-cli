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

Scope: this module exercises *only* the code-upload path via ``snow app deploy
--upload-only``, for both a workspace and a stage. That is the code path behind
the Windows ``file://`` URI fix — each bundled file is sent to its destination
with a ``PUT file://...`` — and the path that has to work for a role that can
write to a stage without owning it. ``--upload-only`` stops before the build
and service phases, so these tests need no compute pool or other
container-service resources and run cheaply against a real account.

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


@pytest.fixture()
def stage_writable_but_not_owned(snowflake_session, unique_stage, test_role):
    """Create a stage the connection's role owns and *test_role* can write to.

    This is the situation the deploy has to handle: the deploying role can put
    files on the stage but cannot replace the stage itself, because it neither
    owns the stage nor may create one in the schema. Only the grants needed to
    reach and write to the stage are given, so the deploy has to cope without
    CREATE STAGE.
    """
    stage = f"{DATABASE}.{SCHEMA}.{unique_stage}"
    snowflake_session.execute_string(
        f"CREATE STAGE {stage} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');"
        f"GRANT USAGE ON DATABASE {DATABASE} TO ROLE {test_role};"
        f"GRANT USAGE ON SCHEMA {DATABASE}.{SCHEMA} TO ROLE {test_role};"
        f"GRANT READ, WRITE ON STAGE {stage} TO ROLE {test_role};"
        f"GRANT USAGE ON WAREHOUSE {WAREHOUSE} TO ROLE {test_role};"
    )
    return unique_stage


@pytest.mark.integration
def test_deploy_upload_only_uploads_to_a_stage_the_role_does_not_own(
    runner,
    temporary_working_directory,
    snowflake_session,
    test_role,
    stage_writable_but_not_owned,
):
    """A role with WRITE but not OWNERSHIP can upload to an existing stage.

    The upload used to begin by dropping the stage and creating it again, which
    such a role cannot do, so it could never deploy to a stage that already
    existed. The stage contents are cleared instead, and the stage itself has
    to survive: recreating it is exactly what this role cannot do.
    """
    stage_name = stage_writable_but_not_owned
    app_name = f"STAGE_REDEPLOY_APP_{uuid.uuid4().hex[:8]}"

    project_dir = Path(temporary_working_directory)
    (project_dir / "app").mkdir(parents=True)
    (project_dir / "app" / "main.py").write_text("print('hello from snowflake app')\n")

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
        [
            "app",
            "deploy",
            "--entity-id",
            "stage_app",
            "--upload-only",
            "--role",
            test_role,
        ]
    )
    assert result.exit_code == 0, f"Upload failed:\n{result.output}"
    assert "instead of recreating it" in result.output
    assert "Artifacts uploaded to" in result.output
    assert "main.py" in result.output

    (stages,) = snowflake_session.execute_string(
        f"SHOW STAGES LIKE '{stage_name}' IN SCHEMA {DATABASE}.{SCHEMA}"
    )
    assert stages.fetchall(), f"The deploy left no stage named {stage_name}"


@pytest.mark.integration
def test_deploy_upload_only_uploads_code_to_workspace(
    runner,
    temporary_working_directory,
    unique_workspace,
):
    """``snow app deploy --upload-only`` uploads bundled files to a workspace.

    Verifies the workspace ``PUT file://...`` upload (including a nested file
    and a glob-metacharacter ``[id]`` directory) succeeds against a real
    account, guarding the local-file-URI construction used by
    ``upload_to_workspace`` from regressions.
    """
    ws_name = unique_workspace
    app_name = f"WS_UPLOAD_APP_{uuid.uuid4().hex[:8]}"

    project_dir = Path(temporary_working_directory)
    (project_dir / "app" / "nested").mkdir(parents=True)
    # A Next.js-style dynamic-route directory: its name contains glob
    # metacharacters, which previously broke the connector's PUT glob (253006).
    (project_dir / "app" / "[id]").mkdir(parents=True)
    (project_dir / "app" / "main.py").write_text("print('hello from snowflake app')\n")
    (project_dir / "app" / "nested" / "util.py").write_text("X = 1\n")
    (project_dir / "app" / "[id]" / "page.tsx").write_text("export default 1\n")

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
    assert os.path.join("[id]", "page.tsx") in result.output


@pytest.mark.integration
def test_deploy_upload_only_uploads_code_to_stage(
    runner,
    temporary_working_directory,
    unique_stage,
):
    """``snow app deploy --upload-only`` uploads bundled files to a stage.

    Verifies the stage ``PUT file://...`` upload (including a nested file and a
    glob-metacharacter ``[id]`` directory) succeeds against a real account. A
    bundle with subdirectories is the case that previously failed with
    connector error 253006 (``Not a file but a directory``) when the stage
    upload globbed ``PUT <dir>/*``; this guards the file-by-file
    ``upload_to_stage`` path from that regression.
    """
    stage_name = unique_stage
    app_name = f"STAGE_UPLOAD_APP_{uuid.uuid4().hex[:8]}"

    project_dir = Path(temporary_working_directory)
    (project_dir / "app" / "nested").mkdir(parents=True)
    (project_dir / "app" / "[id]").mkdir(parents=True)
    (project_dir / "app" / "main.py").write_text("print('hello from snowflake app')\n")
    (project_dir / "app" / "nested" / "util.py").write_text("X = 1\n")
    (project_dir / "app" / "[id]" / "page.tsx").write_text("export default 1\n")

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
    assert os.path.join("[id]", "page.tsx") in result.output
