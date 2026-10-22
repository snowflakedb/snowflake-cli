"""Warm-up tasks that start a Streamlit app on a schedule."""

from typing import Callable, Optional
from unittest import mock

import pytest
from snowflake.cli._plugins.streamlit.streamlit_entity_model import Schedules
from snowflake.cli._plugins.streamlit.warm_up_tasks import WarmUpTasks
from snowflake.cli.api.errno import INVALID_PROPERTY
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.connector.errors import ProgrammingError

APP = FQN(database="DB", schema="PUBLIC", name="MYAPP")
TASK = "DB.PUBLIC.MYAPP__STREAMLIT_LIFECYCLE"
DEFINITION = "SELECT SYSTEM$START_STREAMLIT('DB.PUBLIC.MYAPP')"
SHOW_TASKS = (
    "SHOW TASKS LIKE 'MYAPP\\\\_\\\\_STREAMLIT\\\\_LIFECYCLE%' IN SCHEMA DB.PUBLIC;"
)


def _live(name: str, cron: str, state: str = "started") -> dict:
    return {
        "name": name,
        "database_name": "DB",
        "schema_name": "PUBLIC",
        "schedule": f"USING CRON {cron} UTC",
        "definition": DEFINITION,
        "state": state,
    }


class FakeExecutor:
    def __init__(self, tasks=(), fail: Optional[Callable[[str], bool]] = None):
        self.tasks = list(tasks)
        self.fail = fail or (lambda _: False)
        self.sql: list[str] = []

    def __call__(self, sql: str, **_):
        self.sql.append(sql)
        if self.fail(sql):
            raise ProgrammingError(f"failed: {sql}")
        cursor = mock.Mock()
        cursor.fetchall.return_value = self.tasks if sql.startswith("SHOW") else []
        return cursor

    @property
    def changes(self) -> list[str]:
        return [s for s in self.sql if not s.startswith("SHOW")]


def _sync(executor, cron_exprs, linked, app=APP):
    WarmUpTasks(app=app, execute_query=executor, console=mock.Mock()).sync(
        Schedules(cron_exprs=cron_exprs), linked=linked
    )


def test_new_tasks_are_created_resumed_and_linked():
    executor = FakeExecutor()

    _sync(executor, ["0 9 * * 1-5", "0 13 * * *"], linked=[])

    assert executor.sql[0] == SHOW_TASKS
    assert executor.changes == [
        f"CREATE OR REPLACE TASK IDENTIFIER('{TASK}')\n"
        "SCHEDULE = 'USING CRON 0 9 * * 1-5 UTC'\n"
        f"AS {DEFINITION};",
        f"ALTER TASK IDENTIFIER('{TASK}') RESUME;",
        f"CREATE OR REPLACE TASK IDENTIFIER('{TASK}_2')\n"
        "SCHEDULE = 'USING CRON 0 13 * * * UTC'\n"
        f"AS {DEFINITION};",
        f"ALTER TASK IDENTIFIER('{TASK}_2') RESUME;",
        "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.MYAPP') SET "
        f"SCHEDULED_TASKS = ({TASK}, {TASK}_2);",
    ]


def test_an_unchanged_schedule_sends_no_change():
    executor = FakeExecutor(
        tasks=[
            _live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *"),
            _live("MYAPP__STREAMLIT_LIFECYCLE_2", "0 13 * * *"),
        ]
    )

    _sync(executor, ["0 9 * * *", "0 13 * * *"], linked=[f"{TASK}_2", TASK])

    assert executor.changes == []


def test_a_suspended_task_is_only_resumed():
    executor = FakeExecutor(
        tasks=[_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *", "suspended")]
    )

    _sync(executor, ["0 9 * * *"], linked=[TASK])

    assert executor.changes == [f"ALTER TASK IDENTIFIER('{TASK}') RESUME;"]


def test_only_the_changed_task_is_replaced():
    executor = FakeExecutor(
        tasks=[
            _live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *"),
            _live("MYAPP__STREAMLIT_LIFECYCLE_2", "0 13 * * *"),
        ]
    )

    _sync(executor, ["0 9 * * *", "0 14 * * *"], linked=[TASK, f"{TASK}_2"])

    assert executor.changes == [
        f"CREATE OR REPLACE TASK IDENTIFIER('{TASK}_2')\n"
        "SCHEDULE = 'USING CRON 0 14 * * * UTC'\n"
        f"AS {DEFINITION};",
        f"ALTER TASK IDENTIFIER('{TASK}_2') RESUME;",
    ]


def test_a_removed_schedule_drops_its_task_before_the_link():
    executor = FakeExecutor(
        tasks=[
            _live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *"),
            _live("MYAPP__STREAMLIT_LIFECYCLE_2", "0 13 * * *"),
        ]
    )

    _sync(executor, ["0 9 * * *"], linked=[TASK, f"{TASK}_2"])

    assert executor.changes == [
        f"DROP TASK IF EXISTS IDENTIFIER('{TASK}_2');",
        "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.MYAPP') SET "
        f"SCHEDULED_TASKS = ({TASK});",
    ]


def test_empty_cron_exprs_drop_the_tasks_and_unset_the_link():
    executor = FakeExecutor(tasks=[_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *")])

    _sync(executor, [], linked=[TASK])

    assert executor.changes == [
        f"DROP TASK IF EXISTS IDENTIFIER('{TASK}');",
        "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.MYAPP') UNSET SCHEDULED_TASKS;",
    ]


def test_empty_cron_exprs_with_nothing_live_send_no_change():
    executor = FakeExecutor()

    _sync(executor, [], linked=[])

    assert executor.changes == []


def test_an_unknown_link_is_set_again():
    executor = FakeExecutor(tasks=[_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *")])

    _sync(executor, ["0 9 * * *"], linked=None)

    assert executor.changes == [
        f"ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.MYAPP') SET SCHEDULED_TASKS = ({TASK});"
    ]


def test_a_failed_show_tasks_changes_nothing():
    executor = FakeExecutor(
        tasks=[_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *")],
        fail=lambda sql: sql.startswith("SHOW"),
    )

    with pytest.raises(CliError, match="The CLI did not change the tasks"):
        _sync(executor, ["0 9 * * *"], linked=[TASK])

    assert executor.changes == []


def test_a_failed_link_drops_only_the_tasks_this_deploy_made():
    executor = FakeExecutor(
        tasks=[_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *")],
        fail=lambda sql: "SET SCHEDULED_TASKS" in sql,
    )

    with pytest.raises(CliError, match="link the warm-up tasks"):
        _sync(executor, ["0 9 * * *", "0 13 * * *"], linked=[TASK])

    assert executor.changes[-1] == f"DROP TASK IF EXISTS IDENTIFIER('{TASK}_2');"
    assert f"DROP TASK IF EXISTS IDENTIFIER('{TASK}');" not in executor.changes


def test_a_failed_create_names_the_privileges_and_the_tasks_left_behind():
    executor = FakeExecutor(
        fail=lambda sql: (sql.startswith("CREATE") and f"('{TASK}_2')" in sql)
        or sql.startswith("DROP")
    )

    with pytest.raises(CliError) as error:
        _sync(executor, ["0 9 * * *", "0 13 * * *"], linked=[])

    assert (
        "Schedules need CREATE TASK on the schema, and EXECUTE TASK and "
        "EXECUTE MANAGED TASK on the account."
    ) in str(error.value)
    assert f"may be left behind: {TASK}." in str(error.value)
    assert not any("SCHEDULED_TASKS" in s for s in executor.sql)


def test_lifecycle_controls_off_is_named():
    def execute(sql, **_):
        if "SCHEDULED_TASKS" in sql:
            raise ProgrammingError("Invalid property", errno=INVALID_PROPERTY)
        return FakeExecutor()(sql)

    with pytest.raises(CliError, match="does not have Streamlit lifecycle controls"):
        _sync(execute, ["0 9 * * *"], linked=[])


def test_database_only_app_uses_public_and_ignores_other_schemas():
    app = FQN(database="DB", schema=None, name="MYAPP")
    executor = FakeExecutor(
        tasks=[
            {
                **_live("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *"),
                "database_name": "DB",
                "schema_name": "OTHER",
            },
            {
                **_live("MYAPP__STREAMLIT_LIFECYCLE_2", "0 13 * * *"),
                "database_name": "DB",
                "schema_name": "OTHER",
            },
        ]
    )

    _sync(executor, ["0 9 * * *"], linked=[], app=app)

    assert executor.sql[0] == SHOW_TASKS
    assert executor.changes == [
        f"CREATE OR REPLACE TASK IDENTIFIER('{TASK}')\n"
        "SCHEDULE = 'USING CRON 0 9 * * * UTC'\n"
        f"AS {DEFINITION};",
        f"ALTER TASK IDENTIFIER('{TASK}') RESUME;",
        "ALTER STREAMLIT IDENTIFIER('DB.PUBLIC.MYAPP') SET "
        f"SCHEDULED_TASKS = ({TASK});",
    ]


def test_a_quoted_database_and_schema_match_their_live_tasks():
    app = FQN(database='"mydb"', schema='"My Schema"', name="MYAPP")
    definition = 'SELECT SYSTEM$START_STREAMLIT(\'"mydb"."My Schema".MYAPP\')'
    executor = FakeExecutor(
        tasks=[
            {
                **_live(name, cron),
                "database_name": "mydb",
                "schema_name": "My Schema",
                "definition": definition,
            }
            for name, cron in (
                ("MYAPP__STREAMLIT_LIFECYCLE", "0 9 * * *"),
                ("MYAPP__STREAMLIT_LIFECYCLE_2", "0 13 * * *"),
            )
        ]
    )

    _sync(
        executor,
        ["0 9 * * *"],
        linked=[
            '"mydb"."My Schema".MYAPP__STREAMLIT_LIFECYCLE',
            '"mydb"."My Schema".MYAPP__STREAMLIT_LIFECYCLE_2',
        ],
        app=app,
    )

    # Slot 1 is unchanged. Slot 2 is dropped, not left behind unlinked.
    assert executor.changes == [
        "DROP TASK IF EXISTS "
        'IDENTIFIER(\'"mydb"."My Schema".MYAPP__STREAMLIT_LIFECYCLE_2\');',
        'ALTER STREAMLIT IDENTIFIER(\'"mydb"."My Schema".MYAPP\') SET '
        'SCHEDULED_TASKS = ("mydb"."My Schema".MYAPP__STREAMLIT_LIFECYCLE);',
    ]


@pytest.mark.parametrize(
    "app, scope",
    [
        (FQN(database=None, schema="SCH", name="MYAPP"), " IN SCHEMA SCH"),
        (FQN(database=None, schema=None, name="MYAPP"), ""),
    ],
)
def test_show_tasks_is_scoped_as_the_task_names_are(app, scope):
    executor = FakeExecutor()

    _sync(executor, [], linked=[], app=app)

    assert executor.sql == [
        f"SHOW TASKS LIKE 'MYAPP\\\\_\\\\_STREAMLIT\\\\_LIFECYCLE%'{scope};"
    ]


def test_a_quoted_app_name_matches_its_live_task_and_link():
    app = FQN(database="DB", schema="PUBLIC", name='"My App"')
    executor = FakeExecutor(
        tasks=[
            {
                "name": "My App__STREAMLIT_LIFECYCLE",
                "database_name": "DB",
                "schema_name": "PUBLIC",
                "schedule": "USING CRON 0 9 * * * UTC",
                "definition": "SELECT SYSTEM$START_STREAMLIT('DB.PUBLIC.\"My App\"')",
                "state": "started",
            }
        ]
    )

    _sync(
        executor,
        ["0 9 * * *"],
        # SHOW STREAMLITS keeps the quotes, as seen on a live account.
        linked=['DB.PUBLIC."My App__STREAMLIT_LIFECYCLE"'],
        app=app,
    )

    assert executor.sql == [
        "SHOW TASKS LIKE 'My App\\\\_\\\\_STREAMLIT\\\\_LIFECYCLE%' IN SCHEMA DB.PUBLIC;"
    ]
