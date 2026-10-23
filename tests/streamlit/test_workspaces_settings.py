"""Streamlit settings Snowsight Workspaces writes to snowflake.yml.

``execute_as``, ``app_warehouse``, ``artifact_repositories``,
``idle_auto_shutdown_time_seconds`` and ``schedules`` each change how the
deployed app runs, so each must be applied, not merely accepted.
"""

from pathlib import Path, PureWindowsPath
from textwrap import dedent
from unittest import mock

import pytest
import yaml
from pydantic import ValidationError
from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    WAREHOUSE_RUNTIME_NAME,
    StreamlitEntityModel,
)
from snowflake.cli._plugins.streamlit.warm_up_tasks import (
    WarmUpTasks,
    dotted_sql_name,
)
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.exceptions import CliError
from snowflake.connector.errors import ProgrammingError

from tests.streamlit.streamlit_test_class import STREAMLIT_NAME, StreamlitTestClass

ENTITY = "snowflake.cli._plugins.streamlit.streamlit_entity"
LIVE_URI = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
TASK = f"DB.PUBLIC.{STREAMLIT_NAME}__STREAMLIT_LIFECYCLE"


def _relative_parts(path: str) -> tuple[str, ...]:
    return Path(path.lstrip("/\\")).parts


def _absolute(path: str) -> Path:
    """Anchor a dummy path so ``is_absolute()`` is true on POSIX and Windows.

    A leading slash is absolute on POSIX and only drive-relative on Windows
    (``\\tmp\\project``). ``BundleMap`` rejects that Windows form.
    """
    return Path(Path.cwd().anchor, *_relative_parts(path))


def test_dummy_project_paths_are_absolute_on_windows():
    parts = _relative_parts("/tmp/project")
    windows_root = PureWindowsPath("C:\\", *parts)
    assert windows_root.is_absolute()
    assert windows_root == PureWindowsPath(r"C:\tmp\project")
    assert not PureWindowsPath("/tmp/project").is_absolute()

    root = _absolute("/tmp/project")
    deploy = _absolute("/tmp/project/output")
    assert root.is_absolute()
    assert deploy.is_absolute()
    BundleMap(project_root=root, deploy_root=deploy)


def _model(**fields) -> StreamlitEntityModel:
    model = StreamlitEntityModel(
        type="streamlit",
        identifier={"name": STREAMLIT_NAME, "database": "DB", "schema": "PUBLIC"},
        query_warehouse="test_warehouse",
        main_file="streamlit_app.py",
        artifacts=["streamlit_app.py"],
        **fields,
    )
    model.set_entity_id(STREAMLIT_NAME)
    return model


def _sql_calls(mock_execute) -> list[str]:
    return [call.args[0] for call in mock_execute.call_args_list]


class TestModel:
    def test_workspaces_authored_entity_parses(self):
        """The shape Snowsight Workspaces writes loads without an unknown-key error."""
        definition = yaml.safe_load(
            dedent(
                """
                type: streamlit
                identifier: {name: app, database: DB, schema: PUBLIC}
                main_file: streamlit_app.py
                query_warehouse: WH
                runtime_name: SYSTEM$WAREHOUSE_RUNTIME
                app_warehouse: APP_WH
                execute_as: RESTRICTED CALLER
                idle_auto_shutdown_time_seconds: 86400
                schedules:
                  cron_exprs: ["0 9 * * 1-5"]
                  timezone: America/Los_Angeles
                grants:
                  - privilege: USAGE
                    role: ANALYST
                """
            )
        )

        model = StreamlitEntityModel(**definition)

        assert model.execute_as == "RESTRICTED CALLER"
        assert model.app_warehouse == "APP_WH"
        assert model.idle_auto_shutdown_time_seconds == 86400
        assert model.schedules.cron_exprs == ["0 9 * * 1-5"]
        assert model.schedules.timezone == "America/Los_Angeles"

    @pytest.mark.parametrize(
        "given", ["restricted caller", "  Restricted   Caller ", "RESTRICTED CALLER"]
    )
    def test_execute_as_is_canonicalized(self, given):
        assert _model(execute_as=given).execute_as == "RESTRICTED CALLER"

    @pytest.mark.parametrize("given", ["CALLER", "OWNER; DROP TABLE t", ""])
    def test_execute_as_rejects_anything_else(self, given):
        with pytest.raises(ValidationError):
            _model(execute_as=given)

    @pytest.mark.parametrize("seconds", [0, -1])
    def test_idle_timeout_must_be_positive(self, seconds):
        with pytest.raises(ValidationError):
            _model(idle_auto_shutdown_time_seconds=seconds)

    @pytest.mark.parametrize(
        "fields",
        [
            {"app_warehouse": "  "},
            {"app_warehouse": "WH\\"},
            {"artifact_repositories": [" "]},
            {"artifact_repositories": ["REPO\\"]},
        ],
    )
    def test_blank_or_unquotable_names_are_rejected(self, fields):
        with pytest.raises(ValidationError):
            _model(**fields)

    @pytest.mark.parametrize(
        "schedules",
        [
            {"cron_exprs": ["* * * * *"] * 6},
            {"cron_exprs": [" "]},
            {"cron_exprs": ["0 9 * * *' AS SELECT 1"]},
            {"cron_exprs": ["0 9\n* * *"]},
            {"cron_exprs": ["0 9 * * *"], "timezone": "UTC; DROP"},
            {"cron_exprs": ["0 9 * * *"], "timezone": " "},
            {"cron_exprs": ["0 9 * * *"], "unknown": 1},
        ],
    )
    def test_schedules_reject_values_that_break_the_task_ddl(self, schedules):
        with pytest.raises(ValidationError):
            _model(schedules=schedules)

    def test_templated_values_survive_the_pre_render_pass(self):
        from snowflake.cli.api.project.schemas.updatable_model import context

        with context({"skip_validation_on_templates": True}):
            model = _model(
                execute_as="<% ctx.env.EXECUTE_AS %>",
                app_warehouse="<% ctx.env.APP_WH %>",
            )

        assert model.execute_as == "<% ctx.env.EXECUTE_AS %>"
        assert model.app_warehouse == "<% ctx.env.APP_WH %>"


class TestSql(StreamlitTestClass):
    def _entity(self, workspace_context, **fields) -> StreamlitEntity:
        return StreamlitEntity(
            workspace_ctx=workspace_context, entity_model=_model(**fields)
        )

    def test_create_sets_warehouse_runtime_settings(self, workspace_context):
        entity = self._entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            app_warehouse="app-wh",
            idle_auto_shutdown_time_seconds=86400,
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/a"), legacy=False)

        assert '\nWAREHOUSE = "app-wh"\n' in sql
        assert "\nIDLE_AUTO_SHUTDOWN_TIME_SECONDS = 86400" in sql
        assert "ARTIFACT_REPOSITORIES" not in sql
        assert "EXECUTE AS" not in sql

    @pytest.mark.parametrize("replace", [False, True])
    @pytest.mark.parametrize(
        "execute_as, clause",
        [("RESTRICTED CALLER", "\nEXECUTE AS RESTRICTED CALLER;"), ("OWNER", None)],
    )
    def test_create_sets_restricted_caller(
        self, workspace_context, replace, execute_as, clause
    ):
        entity = self._entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as=execute_as,
            tags=[{"name": "TAG", "value": "x"}],
        )

        sql = entity.get_deploy_sql(
            artifacts_dir=Path("/tmp/a"), legacy=False, replace=replace
        )

        if clause:
            assert sql.endswith(clause)
        else:
            assert "EXECUTE AS" not in sql

    def test_create_sets_artifact_repositories_on_container_runtime(
        self, workspace_context
    ):
        entity = self._entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            compute_pool="POOL",
            artifact_repositories=["snowflake.snowpark.pypi_shared_repository"],
        )

        sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/a"), legacy=False)

        assert (
            "\nARTIFACT_REPOSITORIES = (snowflake.snowpark.pypi_shared_repository)"
            in sql
        )
        assert "\nWAREHOUSE = " not in sql

    def test_omitted_settings_emit_nothing(self, workspace_context):
        entity = self._entity(workspace_context, runtime_name=WAREHOUSE_RUNTIME_NAME)

        create_sql = entity.get_deploy_sql(artifacts_dir=Path("/tmp/a"), legacy=False)
        alter_sql = entity.get_alter_sql(
            current={
                "query_warehouse": "test_warehouse",
                "main_file": "streamlit_app.py",
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "code_warehouse": "LIVE_WH",
                "idle_auto_shutdown_time_seconds": 3600,
            }
        )

        for clause in ("\nWAREHOUSE", "ARTIFACT_REPOSITORIES", "IDLE_AUTO_SHUTDOWN"):
            assert clause not in create_sql
        # A setting left out of snowflake.yml leaves the live value alone.
        assert alter_sql is None

    def test_alter_sets_only_changed_settings(self, workspace_context):
        entity = self._entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            app_warehouse="new_wh",
            idle_auto_shutdown_time_seconds=7200,
        )

        changed = entity.get_alter_sql(
            current={
                "query_warehouse": "test_warehouse",
                "main_file": "streamlit_app.py",
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "code_warehouse": "OLD_WH",
                "idle_auto_shutdown_time_seconds": 3600,
            }
        )
        unchanged = entity.get_alter_sql(
            current={
                "query_warehouse": "test_warehouse",
                "main_file": "streamlit_app.py",
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "code_warehouse": "NEW_WH",
                "idle_auto_shutdown_time_seconds": 7200,
            }
        )

        assert "WAREHOUSE = new_wh" in changed
        assert "IDLE_AUTO_SHUTDOWN_TIME_SECONDS = 7200" in changed
        assert unchanged is None

    def test_alter_compares_artifact_repositories_as_a_set(self, workspace_context):
        entity = self._entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            artifact_repositories=["db.sch.b", "db.sch.a"],
        )
        current = {
            "query_warehouse": "test_warehouse",
            "main_file": "streamlit_app.py",
            "runtime_name": SPCS_RUNTIME_V2_NAME,
        }

        same = entity.get_alter_sql(
            current={**current, "artifact_repositories": '["DB.SCH.A", "DB.SCH.B"]'}
        )
        moved = entity.get_alter_sql(
            current={**current, "artifact_repositories": '["OTHER.SCH.A", "DB.SCH.B"]'}
        )

        assert same is None
        assert "ARTIFACT_REPOSITORIES = (db.sch.b, db.sch.a)" in moved

    @pytest.mark.parametrize(
        "live, expected",
        [
            (
                '["DB.SCH.A"]',
                "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') "
                "UNSET ARTIFACT_REPOSITORIES;",
            ),
            ("[]", None),
        ],
    )
    def test_empty_artifact_repositories_clear_the_live_value(
        self, workspace_context, live, expected
    ):
        entity = self._entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            artifact_repositories=[],
        )

        assert (
            entity.get_alter_sql(
                current={
                    "query_warehouse": "test_warehouse",
                    "main_file": "streamlit_app.py",
                    "runtime_name": SPCS_RUNTIME_V2_NAME,
                    "artifact_repositories": live,
                }
            )
            == expected
        )
        assert "ARTIFACT_REPOSITORIES" not in entity.get_deploy_sql(
            artifacts_dir=Path("/tmp/a"), legacy=False
        )

    @pytest.mark.parametrize("runtime", [WAREHOUSE_RUNTIME_NAME, None])
    def test_empty_artifact_repositories_are_accepted_on_any_runtime(
        self, workspace_context, runtime
    ):
        entity = self._entity(
            workspace_context, runtime_name=runtime, artifact_repositories=[]
        )

        entity._validate_runtime_settings(legacy=True)  # noqa: SLF001
        entity.validate_as_child()

    def test_clears_repositories_before_switching_to_warehouse_runtime(
        self, workspace_context
    ):
        entity = self._entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            artifact_repositories=[],
        )

        alter = entity.get_alter_sql(
            current={
                "query_warehouse": "test_warehouse",
                "main_file": "streamlit_app.py",
                "runtime_name": SPCS_RUNTIME_V2_NAME,
                "artifact_repositories": '["DB.SCH.REPO"]',
            }
        )

        assert alter is not None
        statements = alter.splitlines()
        assert statements[0] == (
            "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') "
            "UNSET ARTIFACT_REPOSITORIES;"
        )
        assert statements[1] == (
            "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') SET"
        )
        assert any(
            statement.rstrip(";") == f"RUNTIME_NAME = '{WAREHOUSE_RUNTIME_NAME}'"
            for statement in statements[2:]
        )
        assert alter.index("UNSET ARTIFACT_REPOSITORIES;") < alter.index(
            "RUNTIME_NAME ="
        )

    def test_show_only_settings_use_public_for_database_only_identifier(
        self, workspace_context
    ):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier={"name": STREAMLIT_NAME, "database": "DB"},
            query_warehouse="test_warehouse",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
            runtime_name=SPCS_RUNTIME_V2_NAME,
            idle_auto_shutdown_time_seconds=86400,
        )
        model.set_entity_id(STREAMLIT_NAME)
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)
        cursor = mock.Mock()
        cursor.fetchall.return_value = []
        self.mock_execute.return_value = cursor

        entity._show_only_settings()  # noqa: SLF001

        assert self.mock_execute.call_args.args[0] == (
            "SHOW STREAMLITS LIKE 'TEST\\\\_STREAMLIT' IN SCHEMA DB.PUBLIC;"
        )

    def test_show_only_settings_are_read_from_show_streamlits(self, workspace_context):
        """DESCRIBE has no artifact_repositories or idle timeout; SHOW does."""
        entity = self._entity(
            workspace_context,
            runtime_name=SPCS_RUNTIME_V2_NAME,
            artifact_repositories=["snowflake.snowpark.pypi_shared_repository"],
            idle_auto_shutdown_time_seconds=86400,
        )
        cursor = mock.Mock()
        cursor.fetchall.return_value = [
            {
                "name": "TEST_STREAMLIT_OTHER",
                "artifact_repositories": "[]",
                "idle_auto_shutdown_time_seconds": None,
            },
            {
                "name": "TEST_STREAMLIT",
                "artifact_repositories": '["snowflake.snowpark.pypi_shared_repository"]',
                "idle_auto_shutdown_time_seconds": 86400,
            },
        ]
        self.mock_execute.return_value = cursor

        live = entity._show_only_settings()  # noqa: SLF001

        assert self.mock_execute.call_args.args[0] == (
            "SHOW STREAMLITS LIKE 'TEST\\\\_STREAMLIT' IN SCHEMA DB.PUBLIC;"
        )
        assert live == {
            "artifact_repositories": '["snowflake.snowpark.pypi_shared_repository"]',
            "idle_auto_shutdown_time_seconds": 86400,
            "scheduled_tasks": None,
        }
        assert (
            entity.get_alter_sql(
                current={
                    "query_warehouse": "test_warehouse",
                    "main_file": "streamlit_app.py",
                    "runtime_name": SPCS_RUNTIME_V2_NAME,
                    **live,
                }
            )
            is None
        )

    def test_show_only_settings_skip_the_query_when_unused(self, workspace_context):
        entity = self._entity(workspace_context, runtime_name=WAREHOUSE_RUNTIME_NAME)

        assert entity._show_only_settings() == {}  # noqa: SLF001
        self.mock_execute.assert_not_called()

    def test_execute_as_sql_is_keywords_not_a_literal(self, workspace_context):
        entity = self._entity(
            workspace_context,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as="restricted caller",
        )

        assert entity.get_execute_as_sql() == (
            "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') "
            "EXECUTE AS RESTRICTED CALLER;"
        )


class TestApplicationPackageChild(StreamlitTestClass):
    @pytest.mark.parametrize(
        "fields",
        [
            {"execute_as": "RESTRICTED CALLER"},
            {
                "runtime_name": SPCS_RUNTIME_V2_NAME,
                "schedules": {"cron_exprs": ["0 9 * * *"]},
            },
            {"idle_auto_shutdown_time_seconds": 86400},
        ],
    )
    def test_settings_are_rejected_in_an_application_package(
        self, workspace_context, fields
    ):
        entity = StreamlitEntity(
            workspace_ctx=workspace_context, entity_model=_model(**fields)
        )

        with pytest.raises(CliError, match="cannot be set on a Streamlit app in an"):
            entity.validate_as_child()


@mock.patch(f"{ENTITY}.StreamlitManager")
@mock.patch(f"{ENTITY}.sync_deploy_root_with_stage")
@mock.patch(f"{ENTITY}.StageManager")
@mock.patch(f"{ENTITY}.StreamlitEntity._is_legacy_deployment", return_value=False)
@mock.patch(f"{ENTITY}.StreamlitEntity.bundle")
class TestDeploy(StreamlitTestClass):
    """Ordering and failure behavior across a whole deploy."""

    def _deploy(
        self, workspace_context, action_context, *, exists, current=None, **fields
    ):
        workspace_context.project_root = _absolute("/tmp/project")
        self._object_exists_patcher.stop()
        with mock.patch(
            f"{ENTITY}.StreamlitEntity._object_exists", return_value=exists
        ), mock.patch(
            f"{ENTITY}.StreamlitEntity._entrypoint_is_bundled", return_value=True
        ):
            describe = mock.Mock()
            describe.fetchone.return_value = {
                "live_version_location_uri": LIVE_URI,
                "query_warehouse": "test_warehouse",
                **(current or {}),
            }
            self.mock_describe.return_value = describe
            entity = StreamlitEntity(
                workspace_ctx=workspace_context, entity_model=_model(**fields)
            )
            return entity.action_deploy(
                action_context, _open=False, replace=True, legacy=False
            )

    def _setup(self, mock_bundle, mock_sync, mock_manager, order, changed=False):
        mock_bundle.return_value = BundleMap(
            project_root=_absolute("/tmp/project"),
            deploy_root=_absolute("/tmp/project/output"),
        )
        mock_sync.side_effect = lambda **_: order.append("upload") or mock.Mock(
            has_changes=lambda: changed
        )
        self.mock_execute_with_params.side_effect = lambda *_: order.append("restart")
        mock_manager.return_value.grant_privileges.side_effect = (
            lambda *_: order.append("grants")
        )
        self.mock_execute.side_effect = lambda sql, *a, **k: order.append(sql) or (
            mock.Mock(fetchall=lambda: []) if sql.startswith("SHOW") else None
        )

    def test_new_app_gets_restricted_caller_only_from_the_create(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=False,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as="RESTRICTED CALLER",
        )

        [statement] = [s for s in order if "EXECUTE AS" in s]
        assert statement.startswith("CREATE STREAMLIT")

    def test_kept_app_execute_as_runs_before_upload_and_grants(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=True,
            current={"runtime_name": WAREHOUSE_RUNTIME_NAME, "execute_as": "OWNER"},
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as="RESTRICTED CALLER",
        )

        execute_as = order.index(
            "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') "
            "EXECUTE AS RESTRICTED CALLER;"
        )
        assert execute_as < order.index("upload") < order.index("grants")

    def test_execute_as_skipped_when_kept_app_already_matches(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=True,
            current={
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "execute_as": "RESTRICTED CALLER",
            },
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as="RESTRICTED CALLER",
        )

        assert not any("EXECUTE AS" in s for s in order)

    @pytest.mark.parametrize(
        "exists, current, expect_alter",
        [
            # A new app gets its rights from the CREATE.
            (False, None, False),
            # A kept app on restricted caller's rights is moved back.
            (True, {"execute_as": "RESTRICTED CALLER"}, True),
            # DESCRIBE leaves the column out when the account has no RCR.
            (True, {}, False),
        ],
    )
    def test_execute_as_owner_alters_only_an_app_not_on_owner(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
        exists,
        current,
        expect_alter,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=exists,
            current={"runtime_name": WAREHOUSE_RUNTIME_NAME, **(current or {})},
            runtime_name=WAREHOUSE_RUNTIME_NAME,
            execute_as="OWNER",
        )

        altered = [s for s in order if "EXECUTE AS" in s]
        assert altered == (
            [
                "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.test_streamlit') "
                "EXECUTE AS OWNER;"
            ]
            if expect_alter
            else []
        )

    def test_warehouse_rcr_app_moves_to_owner_before_its_runtime_changes(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=True,
            current={
                "runtime_name": WAREHOUSE_RUNTIME_NAME,
                "execute_as": "RESTRICTED CALLER",
            },
            runtime_name=SPCS_RUNTIME_V2_NAME,
            execute_as="OWNER",
        )

        execute_as = [i for i, s in enumerate(order) if "EXECUTE AS" in s]
        runtime = next(i for i, s in enumerate(order) if "RUNTIME_NAME" in s)
        assert len(execute_as) == 1 and execute_as[0] < runtime

    def test_container_app_with_a_stale_rcr_gets_no_execute_as(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        # DESCRIBE keeps RESTRICTED CALLER after a move to a container runtime,
        # and a container runtime rejects every EXECUTE AS.
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=True,
            current={
                "runtime_name": SPCS_RUNTIME_V2_NAME,
                "execute_as": "RESTRICTED CALLER",
            },
            runtime_name=SPCS_RUNTIME_V2_NAME,
            execute_as="OWNER",
        )

        assert not any("EXECUTE AS" in s for s in order)

    def test_execute_as_failure_aborts_before_grants(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        def execute(sql, *a, **k):
            order.append(sql)
            if "EXECUTE AS" in sql:
                raise ProgrammingError("invalid property")

        self.mock_execute.side_effect = execute

        with pytest.raises(ProgrammingError):
            self._deploy(
                workspace_context,
                action_context,
                exists=True,
                current={"runtime_name": WAREHOUSE_RUNTIME_NAME},
                runtime_name=WAREHOUSE_RUNTIME_NAME,
                execute_as="RESTRICTED CALLER",
            )

        assert "upload" not in order and "grants" not in order

    def test_restart_runs_before_a_schedule_failure(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        # A retry finds no stage change and does not restart, so the restart
        # must not wait for the schedule step.
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order, changed=True)

        def execute(sql, *a, **k):
            order.append(sql)
            if sql.startswith("CREATE OR REPLACE TASK"):
                raise ProgrammingError("insufficient privileges")
            return mock.Mock(fetchall=lambda: [])

        self.mock_execute.side_effect = execute

        with pytest.raises(CliError, match="Could not create a warm-up task"):
            self._deploy(
                workspace_context,
                action_context,
                exists=True,
                current={"runtime_name": SPCS_RUNTIME_V2_NAME},
                runtime_name=SPCS_RUNTIME_V2_NAME,
                schedules={"cron_exprs": ["0 9 * * *"]},
            )

        assert order.index("upload") < order.index("restart") < order.index("grants")

    @pytest.mark.parametrize(
        "exists, current, linked",
        [
            (False, None, []),
            (
                True,
                {"scheduled_tasks": f'["{TASK}"]'},
                [TASK],
            ),
            (True, {}, None),
        ],
    )
    def test_schedules_get_the_live_link(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
        exists,
        current,
        linked,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        with mock.patch(f"{ENTITY}.WarmUpTasks") as tasks, mock.patch(
            f"{ENTITY}.StreamlitEntity._show_only_settings", return_value=current or {}
        ):
            self._deploy(
                workspace_context,
                action_context,
                exists=exists,
                current={"runtime_name": SPCS_RUNTIME_V2_NAME},
                runtime_name=SPCS_RUNTIME_V2_NAME,
                schedules={"cron_exprs": ["0 9 * * *"]},
            )

        assert tasks.return_value.sync.call_args.kwargs == {"linked": linked}

    def test_omitted_schedules_leave_tasks_alone(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        self._deploy(
            workspace_context,
            action_context,
            exists=False,
            runtime_name=WAREHOUSE_RUNTIME_NAME,
        )

        assert not any("TASK" in s for s in order if isinstance(s, str))

    def test_setting_the_account_rejects_names_the_yaml_key(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        """The server's bare "invalid property" (seen live on preprod) is explained."""
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        def execute(sql, *a, **k):
            order.append(sql)
            if "CREATE STREAMLIT" in sql:
                raise ProgrammingError(
                    msg="SQL compilation error:\ninvalid property 'WAREHOUSE' for 'STREAMLIT'",
                    errno=1420,
                )

        self.mock_execute.side_effect = execute

        with pytest.raises(CliError, match="apply app_warehouse in snowflake.yml"):
            self._deploy(
                workspace_context,
                action_context,
                exists=False,
                runtime_name=WAREHOUSE_RUNTIME_NAME,
                app_warehouse="APP_WH",
            )

    def test_other_invalid_property_errors_are_raised_unchanged(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)

        def execute(sql, *a, **k):
            order.append(sql)
            if "CREATE STREAMLIT" in sql:
                raise ProgrammingError(
                    msg="SQL compilation error:\ninvalid property 'COMPUTE_POOL' for 'STREAMLIT'",
                    errno=1420,
                )

        self.mock_execute.side_effect = execute

        with pytest.raises(ProgrammingError, match="COMPUTE_POOL"):
            self._deploy(
                workspace_context,
                action_context,
                exists=False,
                runtime_name=WAREHOUSE_RUNTIME_NAME,
                app_warehouse="APP_WH",
            )

    @pytest.mark.parametrize(
        "runtime, fields, message, legacy",
        [
            (
                None,
                {"idle_auto_shutdown_time_seconds": 86400},
                "cannot be set on a --legacy deployment",
                True,
            ),
            (
                SPCS_RUNTIME_V2_NAME,
                {"execute_as": "RESTRICTED CALLER"},
                "execute_as RESTRICTED CALLER only applies",
                False,
            ),
            (
                WAREHOUSE_RUNTIME_NAME,
                {"artifact_repositories": ["REPO"]},
                "artifact_repositories only applies",
                False,
            ),
            (
                WAREHOUSE_RUNTIME_NAME,
                {"schedules": {"cron_exprs": ["0 9 * * *"]}},
                "schedules only applies",
                False,
            ),
            (
                None,
                {"schedules": {"cron_exprs": ["0 9 * * *"]}},
                "schedules only applies",
                False,
            ),
        ],
    )
    def test_settings_on_the_wrong_runtime_fail_before_any_sql(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        mock_manager,
        workspace_context,
        action_context,
        runtime,
        fields,
        message,
        legacy,
    ):
        order: list[str] = []
        self._setup(mock_bundle, mock_sync, mock_manager, order)
        # The real existence check, so that its DESCRIBE is seen.
        self._object_exists_patcher.stop()
        workspace_context.project_root = _absolute("/tmp/project")
        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=_model(runtime_name=runtime, **fields),
        )

        with pytest.raises(CliError, match=message):
            entity.action_deploy(
                action_context, _open=False, replace=True, legacy=legacy
            )

        self.mock_describe.assert_not_called()
        assert order == []


class TestWorkspacesContainerApp(StreamlitTestClass):
    """The most common file that Snowsight Workspaces writes: a container app."""

    @mock.patch(f"{ENTITY}.StreamlitManager")
    @mock.patch(f"{ENTITY}.sync_deploy_root_with_stage")
    @mock.patch(f"{ENTITY}.StageManager")
    @mock.patch(f"{ENTITY}.StreamlitEntity._is_legacy_deployment", return_value=False)
    @mock.patch(f"{ENTITY}.StreamlitEntity.bundle")
    def test_owner_and_stale_app_warehouse_deploy_on_container_runtime(
        self,
        mock_bundle,
        _mock_is_legacy,
        _mock_stage_manager,
        mock_sync,
        _mock_manager,
        workspace_context,
        action_context,
    ):
        mock_bundle.return_value = BundleMap(
            project_root=_absolute("/tmp/project"),
            deploy_root=_absolute("/tmp/project/output"),
        )
        mock_sync.return_value = mock.Mock(has_changes=lambda: False)
        workspace_context.project_root = _absolute("/tmp/project")
        entity = StreamlitEntity(
            workspace_ctx=workspace_context,
            entity_model=_model(
                runtime_name=SPCS_RUNTIME_V2_NAME,
                compute_pool="POOL",
                execute_as="OWNER",
                app_warehouse="STALE_WH",
            ),
        )

        entity.action_deploy(action_context, _open=False, replace=True, legacy=False)

        sql = "\n".join(_sql_calls(self.mock_execute))
        assert "EXECUTE AS" not in sql
        assert "\nWAREHOUSE = " not in sql
        workspace_context.console.warning.assert_any_call(
            "app_warehouse STALE_WH is ignored because runtime_name is "
            f"{SPCS_RUNTIME_V2_NAME}. Remove app_warehouse, or set runtime_name to "
            f"{WAREHOUSE_RUNTIME_NAME}."
        )


class TestScheduleTaskNames(StreamlitTestClass):
    @pytest.mark.parametrize(
        "name, slot, task_sql, task_dotted",
        [
            (
                "MYAPP",
                0,
                "IDENTIFIER('DB.PUBLIC.MYAPP__STREAMLIT_LIFECYCLE')",
                "DB.PUBLIC.MYAPP__STREAMLIT_LIFECYCLE",
            ),
            (
                '"My App"',
                1,
                "IDENTIFIER('DB.PUBLIC.\"My App__STREAMLIT_LIFECYCLE_2\"')",
                'DB.PUBLIC."My App__STREAMLIT_LIFECYCLE_2"',
            ),
            (
                '"my_app"',
                0,
                "IDENTIFIER('DB.PUBLIC.\"my_app__STREAMLIT_LIFECYCLE\"')",
                'DB.PUBLIC."my_app__STREAMLIT_LIFECYCLE"',
            ),
        ],
    )
    def test_suffix_stays_inside_the_quotes(
        self, workspace_context, name, slot, task_sql, task_dotted
    ):
        model = StreamlitEntityModel(
            type="streamlit",
            identifier={"name": name, "database": "DB", "schema": "PUBLIC"},
            query_warehouse="test_warehouse",
            main_file="streamlit_app.py",
            artifacts=["streamlit_app.py"],
        )
        model.set_entity_id("app")
        entity = StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)

        task = WarmUpTasks(
            app=entity._get_fqn(),  # noqa: SLF001
            execute_query=mock.Mock(),
            console=mock.Mock(),
        ).task_fqn(slot)

        assert task.sql_identifier == task_sql
        assert dotted_sql_name(task) == task_dotted
