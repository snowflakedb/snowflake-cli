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

"""Integration tests for ``snow app deploy`` (Snowflake Apps Deploy).

The fixture project is a minimal **Next.js** standalone app (``app.yml`` +
``npm ci`` / ``next build``), aligned with
``snowflake-app-services/demos/integration-test``.

Run the end-to-end deploy test alone (avoids pool contention). Prefer Hatch::

    hatch run integration:test_snowflake_apps_deploy_e2e

That enables ``-s`` so build-log poller output appears in CI logs. The test is
marked ``snow_app_deploy_debug`` so CI on branch ``gbloom/snow-app-deploy-tests``
runs only this integration test for faster feedback.
"""

from __future__ import annotations

import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from snowflake.connector.cursor import DictCursor

from snowflake.cli._plugins.apps.manager import _poll_until as _poll_until_impl

from tests_integration.apps.conftest import COMPUTE_POOL, DATABASE
from tests_integration.snowflake_connector import new_integration_connection

IMAGE_REPO_NAME = "SNOW_APPS_DEFAULT_IMAGE_REPOSITORY"

# Match ``_poll_until`` defaults (5s interval): 180 * 5s = 15 minutes cap per wait.
_DEPLOY_POLL_MAX_ATTEMPTS = 180


def _poll_until_15min(*args, **kwargs):
    kwargs.setdefault("max_attempts", _DEPLOY_POLL_MAX_ATTEMPTS)
    kwargs.setdefault("interval_seconds", 5)
    return _poll_until_impl(*args, **kwargs)


def _print_build_job_logs(database: str, schema_name: str, conn) -> None:
    """Print ``SPCS_GET_LOGS()`` for non-application services in the test schema."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = None
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(f"SHOW SERVICES IN SCHEMA {database}.{schema_name}")
        rows = cur.fetchall()
    except Exception as exc:
        print(f"\n[{ts}] build-log poll: SHOW SERVICES failed: {exc}", flush=True)
        return
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass
    names = [r["name"] for r in rows]
    statuses = [r.get("status") for r in rows]
    print(
        f"\n[{ts}] build-log poll: services in {database}.{schema_name}: "
        f"names={names!r} statuses={statuses!r}",
        flush=True,
    )
    upper_names = {n: n.upper() for n in names}
    candidates = [
        n
        for n in names
        if n != "TEST_APP"
        and (
            "SPCS_APP_BUILDER" in upper_names[n]
            or "BUILD_JOB" in upper_names[n]
            or "BUILD" in upper_names[n]
        )
    ]
    if not candidates:
        candidates = [n for n in names if n != "TEST_APP"]
    for svc in candidates[:5]:
        fqn = f"{database}.{schema_name}.{svc}"
        _print_service_describe(ts, fqn, conn)
        _print_service_containers(ts, fqn, conn)
        try:
            sql = f"SELECT * FROM TABLE({fqn}!SPCS_GET_LOGS())"
            result = conn.execute_string(sql)
            out_cur = result[-1]
            log_rows = out_cur.fetchall()
            print(
                f"[{ts}] SPCS_GET_LOGS({fqn}): {len(log_rows)} row(s)",
                flush=True,
            )
            for lr in log_rows[:50]:
                print(f"  {lr}", flush=True)
            if len(log_rows) > 50:
                print(f"  ... ({len(log_rows) - 50} more rows)", flush=True)
        except Exception as exc:
            print(f"[{ts}] SPCS_GET_LOGS({svc}) failed: {exc}", flush=True)


def _print_service_describe(ts: str, fqn: str, conn) -> None:
    """Print ``DESCRIBE SERVICE <fqn>`` rows for a build job service."""
    cur = None
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(f"DESCRIBE SERVICE {fqn}")
        rows = cur.fetchall()
        print(
            f"[{ts}] DESCRIBE SERVICE {fqn}: {len(rows)} row(s)",
            flush=True,
        )
        for r in rows:
            print(f"  {r}", flush=True)
    except Exception as exc:
        print(f"[{ts}] DESCRIBE SERVICE {fqn} failed: {exc}", flush=True)
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass


def _print_service_containers(ts: str, fqn: str, conn) -> None:
    """Print ``SHOW SERVICE CONTAINERS IN SERVICE <fqn>`` rows."""
    cur = None
    try:
        cur = conn.cursor(DictCursor)
        cur.execute(f"SHOW SERVICE CONTAINERS IN SERVICE {fqn}")
        rows = cur.fetchall()
        print(
            f"[{ts}] SHOW SERVICE CONTAINERS IN SERVICE {fqn}: {len(rows)} row(s)",
            flush=True,
        )
        for r in rows:
            print(f"  {r}", flush=True)
    except Exception as exc:
        print(
            f"[{ts}] SHOW SERVICE CONTAINERS IN SERVICE {fqn} failed: {exc}",
            flush=True,
        )
    finally:
        if cur is not None:
            try:
                cur.close()
            except Exception:
                pass


def _build_log_poll_worker(
    stop: threading.Event, database: str, schema_name: str
) -> None:
    try:
        conn = new_integration_connection()
    except Exception as exc:
        print(f"build-log poller: could not open connection: {exc}", flush=True)
        return
    try:
        while not stop.is_set():
            _print_build_job_logs(database, schema_name, conn)
            if stop.wait(timeout=60):
                break
    finally:
        conn.close()


@pytest.fixture()
def snowflake_apps_test_schema(snowflake_session):
    """Provision a unique schema + image repo for one deploy test, drop on teardown."""
    uid = uuid.uuid4().hex[:8]
    schema_name = f"SNOW_APP_TEST_{uid}"
    snowflake_session.execute_string(
        f"CREATE SCHEMA IF NOT EXISTS {DATABASE}.{schema_name}"
    )
    snowflake_session.execute_string(
        f"CREATE IMAGE REPOSITORY IF NOT EXISTS "
        f"{DATABASE}.{schema_name}.{IMAGE_REPO_NAME}"
    )
    try:
        yield schema_name
    finally:
        for stmt in (
            f"DROP APPLICATION SERVICE IF EXISTS {DATABASE}.{schema_name}.TEST_APP",
            f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.TEST_APP",
            f"DROP SERVICE IF EXISTS {DATABASE}.{schema_name}.TEST_APP_BUILD_JOB",
            f"DROP STAGE IF EXISTS {DATABASE}.{schema_name}.TEST_APP_CODE",
            (
                "DROP ARTIFACT REPOSITORY IF EXISTS "
                f"{DATABASE}.{schema_name}.SNOW_APPS_DEFAULT_ARTIFACT_REPOSITORY"
            ),
            f"DROP IMAGE REPOSITORY IF EXISTS {DATABASE}.{schema_name}.{IMAGE_REPO_NAME}",
            f"DROP SCHEMA IF EXISTS {DATABASE}.{schema_name}",
        ):
            try:
                snowflake_session.execute_string(stmt)
            except Exception:
                pass


@pytest.mark.integration
@pytest.mark.snowflake_apps_deploy_e2e
@pytest.mark.snow_app_deploy_debug
def test_deploy_end_to_end(
    runner,
    snowflake_session,
    project_directory,
    alter_snowflake_yml,
    snowflake_apps_test_schema,
):
    """Full deploy: upload + build + deploy.

    Asserts the CLI reports an endpoint URL and the application service exists
    in the test schema.

    Uses a 5-minute cap on each internal deploy wait (via patched
    ``_poll_until``), ``--verbose`` on the CLI, and a background thread that prints
    ``SPCS_GET_LOGS()`` for build-related services once per minute.
    """
    schema_name = snowflake_apps_test_schema
    stop_logs = threading.Event()
    log_thread = threading.Thread(
        target=_build_log_poll_worker,
        args=(stop_logs, DATABASE, schema_name),
        name="snowflake-apps-build-log-poller",
        daemon=True,
    )
    log_thread.start()

    with project_directory("snowflake_apps_nextjs") as project_dir:
        yml_path = Path(project_dir) / "snowflake.yml"
        alter_snowflake_yml(yml_path, "entities.test_app.identifier.database", DATABASE)
        alter_snowflake_yml(
            yml_path, "entities.test_app.identifier.schema", schema_name
        )
        alter_snowflake_yml(
            yml_path, "entities.test_app.build_compute_pool.name", COMPUTE_POOL
        )
        alter_snowflake_yml(
            yml_path, "entities.test_app.service_compute_pool.name", COMPUTE_POOL
        )
        # Do not set build_eai: npm/Next builds do not require a named EAI in
        # typical integration accounts (see prior cli_test_integration failures).

        try:
            with patch(
                "snowflake.cli._plugins.apps.commands._poll_until",
                side_effect=_poll_until_15min,
            ):
                result = runner.invoke_with_connection(
                    [
                        "app",
                        "deploy",
                        "--entity-id",
                        "test_app",
                        "--verbose",
                    ]
                )
        finally:
            stop_logs.set()
            log_thread.join(timeout=120)

        assert result.exit_code == 0, result.output
        assert "App ready at" in result.output

        verify_cur = snowflake_session.cursor(DictCursor)
        try:
            verify_cur.execute(f"SHOW SERVICES IN SCHEMA {DATABASE}.{schema_name}")
            service_rows = verify_cur.fetchall()
        finally:
            verify_cur.close()
        service_names = [r["name"] for r in service_rows]
        assert "TEST_APP" in service_names, service_names
