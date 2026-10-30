# Copyright (c) 2024 Snowflake Inc.
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

from __future__ import annotations

import json
import logging
from typing import Any, Callable, Dict, Optional

from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    MAX_SCHEDULES,
    Schedules,
)
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.errno import INVALID_PROPERTY
from snowflake.cli.api.exceptions import CliError, FQNNameError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import (
    append_to_identifier,
    escape_like_pattern,
    to_identifier,
    to_string_literal,
    unquote_identifier,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

log = logging.getLogger(__name__)

# Snowsight Workspaces uses the same task names.
_SCHEDULE_TASK_SUFFIX = "__STREAMLIT_LIFECYCLE"


def dotted_sql_name(fqn: FQN) -> str:
    """A database without a schema means PUBLIC, as in ``FQN.prefix``."""
    return ".".join(to_identifier(part) for part in _dotted_parts(fqn))


def _dotted_parts(fqn: FQN) -> list[str]:
    if fqn.database:
        return [fqn.database, fqn.schema or "PUBLIC", fqn.name]
    return [part for part in (fqn.schema, fqn.name) if part]


def show_scope(fqn: FQN) -> str:
    """The IN clause of a SHOW for objects next to ``fqn``.

    Without IN, SHOW reads the whole current database, so it can find an
    object with the same name in another schema.
    """
    parts = _dotted_parts(fqn)[:-1]
    return f" IN SCHEMA {'.'.join(to_identifier(p) for p in parts)}" if parts else ""


def _name_key(fqn: FQN) -> tuple[str, ...]:
    return tuple(unquote_identifier(part) for part in _dotted_parts(fqn))


def _linked_key(name: str) -> Optional[tuple[str, ...]]:
    """SHOW STREAMLITS keeps the quotes of quoted parts in scheduled_tasks."""
    try:
        return _name_key(FQN.from_string(name))
    except FQNNameError:
        return None


def parse_scheduled_tasks(value: Any) -> Optional[list[str]]:
    """None if the value is not known."""
    if value is None:
        return None
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return None
    return [str(name) for name in value] if isinstance(value, list) else None


def _matches(shown: Optional[str], identifier: Optional[str]) -> bool:
    """SHOW gives names as stored, so only the CLI identifier is unquoted."""
    return (
        shown is not None
        and identifier is not None
        and shown == unquote_identifier(identifier)
    )


class WarmUpTasks:
    """The tasks that start a Streamlit app on a schedule.

    DROP STREAMLIT does not drop these tasks.
    """

    def __init__(
        self,
        app: FQN,
        execute_query: Callable[..., SnowflakeCursor],
        console: AbstractConsole,
    ):
        schema = app.schema or ("PUBLIC" if app.database else None)
        self._app = FQN(
            database=app.database,
            schema=schema,
            name=app.name,
            signature=app.signature,
        )
        self._execute_query = execute_query
        self._console = console

    def task_fqn(self, slot: int) -> FQN:
        """Fixed names let the CLI drop unused tasks without a list query."""
        suffix = _SCHEDULE_TASK_SUFFIX + (f"_{slot + 1}" if slot > 0 else "")
        return FQN(
            database=self._app.database,
            schema=self._app.schema,
            name=append_to_identifier(to_identifier(self._app.name), suffix),
        )

    def _live_tasks(self) -> Dict[str, Dict[str, Any]]:
        """The live warm-up tasks by name.

        If SHOW TASKS fails, the CLI cannot know which tasks are the same, so it
        changes nothing.
        """
        prefix = unquote_identifier(self._app.name) + _SCHEDULE_TASK_SUFFIX
        scope = show_scope(self._app)
        try:
            rows = self._execute_query(
                f"SHOW TASKS LIKE {to_string_literal(escape_like_pattern(prefix) + '%')}"
                f"{scope};",
                cursor_class=DictCursor,
            ).fetchall()
        except ProgrammingError as exc:
            raise CliError(
                f"Could not list the warm-up tasks of Streamlit "
                f"{self._app.sql_identifier}: {sanitize_for_terminal(str(exc))}. "
                "The CLI did not change the tasks. Deploy again when the problem "
                "is fixed."
            ) from exc
        return {
            row["name"]: row
            for row in rows
            if (
                not self._app.database
                or _matches(row.get("database_name"), self._app.database)
            )
            and (
                not self._app.schema
                or _matches(row.get("schema_name"), self._app.schema)
            )
        }

    def _drop(self, tasks: list[FQN]) -> list[str]:
        undropped = []
        for task in tasks:
            try:
                self._execute_query(f"DROP TASK IF EXISTS {task.sql_identifier};")
            except ProgrammingError as exc:
                log.warning("Could not drop warm-up task %s: %s", task.identifier, exc)
                undropped.append(task.identifier)
        return undropped

    def sync(self, schedules: Schedules, linked: Optional[list[str]]) -> None:
        """Change only the tasks and the link that are different from snowflake.yml.

        ``linked`` is the live SCHEDULED_TASKS of the app, or None if it is not
        known. If a step fails, the CLI drops the tasks that it made, so that
        the app does not link to a task that does not run.
        """
        app = self._app
        timezone = schedules.timezone or "UTC"
        definition = (
            f"SELECT SYSTEM$START_STREAMLIT({to_string_literal(dotted_sql_name(app))})"
        )
        live = self._live_tasks()
        created: list[FQN] = []
        wanted: list[FQN] = []

        def fail(step: str, exc: Exception) -> None:
            undropped = self._drop(created)
            if undropped:
                note = (
                    " The CLI could not drop these tasks, so they may be left behind: "
                    f"{', '.join(sanitize_for_terminal(t) for t in undropped)}."
                )
            elif created:
                note = (
                    " The CLI dropped the tasks that it replaced, so the app may have "
                    "no working schedule. Deploy again when the problem is fixed."
                )
            else:
                note = ""
            if getattr(exc, "errno", None) == INVALID_PROPERTY:
                cause = "This account does not have Streamlit lifecycle controls on."
            else:
                # The tasks have no warehouse, so they are serverless.
                cause = (
                    "Schedules need CREATE TASK on the schema, and EXECUTE TASK and "
                    "EXECUTE MANAGED TASK on the account."
                )
            raise CliError(
                f"Could not {step} for Streamlit {app.sql_identifier}: "
                f"{sanitize_for_terminal(str(exc))}. {cause}{note}"
            ) from exc

        for slot, cron in enumerate(schedules.cron_exprs):
            task = self.task_fqn(slot)
            wanted.append(task)
            schedule = f"USING CRON {cron} {timezone}"
            current = live.get(unquote_identifier(task.name))
            same = (
                current is not None
                and current.get("schedule") == schedule
                and current.get("definition") == definition
            )
            if not same:
                self._console.step(
                    f"Creating warm-up task {sanitize_for_terminal(task.identifier)}"
                )
                try:
                    self._execute_query(
                        f"CREATE OR REPLACE TASK {task.sql_identifier}\n"
                        f"SCHEDULE = {to_string_literal(schedule)}\n"
                        f"AS {definition};"
                    )
                except ProgrammingError as exc:
                    fail("create a warm-up task", exc)
                created.append(task)
            # A new or replaced task is suspended.
            if (
                current is None
                or not same
                or str(current.get("state")).lower() != "started"
            ):
                try:
                    self._execute_query(f"ALTER TASK {task.sql_identifier} RESUME;")
                except ProgrammingError as exc:
                    fail("resume a warm-up task", exc)

        # Drop before the link, so that the app does not link to a dropped task.
        unused = [
            task
            for task in (
                self.task_fqn(slot) for slot in range(len(wanted), MAX_SCHEDULES)
            )
            if unquote_identifier(task.name) in live
        ]
        undropped = self._drop(unused)
        if undropped:
            fail(
                "drop the warm-up tasks no longer in snowflake.yml",
                CliError(", ".join(undropped)),
            )

        if linked is not None and sorted(
            _linked_key(name) or () for name in linked
        ) == sorted(_name_key(task) for task in wanted):
            return
        try:
            if wanted:
                # SCHEDULED_TASKS needs dotted names, not IDENTIFIER(...).
                tasks = ", ".join(dotted_sql_name(task) for task in wanted)
                self._execute_query(
                    f"ALTER STREAMLIT {app.sql_identifier} SET SCHEDULED_TASKS = ({tasks});"
                )
            else:
                # UNSET also works when the account turns off lifecycle controls.
                self._execute_query(
                    f"ALTER STREAMLIT {app.sql_identifier} UNSET SCHEDULED_TASKS;"
                )
        except ProgrammingError as exc:
            fail("link the warm-up tasks to the app", exc)
