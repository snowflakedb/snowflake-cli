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
from typing import Any, List, Optional, Sequence, Tuple

from snowflake.cli._plugins.connection.util import make_snowsight_url
from snowflake.cli._plugins.object.manager import ObjectManager
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    StreamlitEntityModel,
    is_spcs_container_runtime,
)
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.cli.api.project.util import (
    to_identifier,
    to_string_literal,
    unquote_identifier,
)
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import DictCursor, SnowflakeCursor

log = logging.getLogger(__name__)

# Shared and per-job Streamlit services are both named stplatstreamlit{id}.
# DESCRIBE does not return the id, so the lookup is SHOW plus the managing object.
_SERVICE_NAME_LIKE = "STPLATSTREAMLIT%"
_MANAGING_OBJECT_DOMAIN = "STREAMLIT"
_SERVICE_NOT_APPLICABLE = "NOT_APPLICABLE"
_SERVICE_NOT_STARTED = "NOT_STARTED"
_MONITOR_MESSAGE = (
    "container_status requires the MONITOR privilege on the service. "
    "service_status is from SHOW SERVICES."
)
_NOT_STARTED_MESSAGE = (
    "No SPCS service is currently attributed to this app. "
    "The service is created when the app runs and can be removed after it is idle."
)
_WAREHOUSE_MESSAGE = "This runtime has no SPCS service."


def _location_identifiers(app_fqn: FQN) -> List[Tuple[str, str]]:
    """``(object type, IDENTIFIER(...))`` for the app's database and schema.

    Each part is quoted on its own before being joined, since `FQN.prefix`
    would substitute `PUBLIC` for a missing schema.
    """
    if not app_fqn.database:
        return []
    database = to_identifier(app_fqn.database)
    locations = [("DATABASE", f"IDENTIFIER({to_string_literal(database)})")]
    if app_fqn.schema:
        qualified = f"{database}.{to_identifier(app_fqn.schema)}"
        locations.append(("SCHEMA", f"IDENTIFIER({to_string_literal(qualified)})"))
    return locations


class StreamlitManager(SqlExecutionMixin):
    def execute(self, app_name: FQN):
        query = f"EXECUTE STREAMLIT {app_name.sql_identifier}()"
        return self.execute_query(query=query)

    def share(
        self,
        streamlit_name: FQN,
        grantees: Sequence[Grant],
        with_grant_option: bool = False,
    ) -> List[SnowflakeCursor]:
        """Grant USAGE on the app to each grantee, one statement each."""
        grant_option = " WITH GRANT OPTION" if with_grant_option else ""
        return [
            self.execute_query(
                f"GRANT USAGE ON STREAMLIT {streamlit_name.sql_identifier}"
                f" TO {grantee.grantee_sql}{grant_option}"
            )
            for grantee in grantees
        ]

    def grant_location_usage(
        self, app_fqn: FQN, grantees: Sequence[Grant]
    ) -> List[str]:
        """Grant USAGE on the app's database and schema, best effort.

        Returns one message per refusal. `GRANT USAGE ON STREAMLIT` does not
        depend on these grants, so the share stands and the caller warns.

        Grants are issued without checking for an existing one: `SHOW GRANTS ON
        DATABASE` lists no users at all, and does not expand role inheritance,
        so a check would report a gap that isn't there.
        """
        problems: List[str] = []
        for object_type, identifier in _location_identifiers(app_fqn):
            for grantee in grantees:
                try:
                    self.execute_query(
                        f"GRANT USAGE ON {object_type} {identifier}"
                        f" TO {grantee.grantee_sql}"
                    )
                except ProgrammingError as error:
                    problems.append(
                        f"Could not grant USAGE on {object_type.lower()}"
                        f" to {sanitize_for_terminal(grantee.grantee_sql)}:"
                        f" {sanitize_for_terminal(str(error.msg))}"
                    )
        return problems

    def grant_privileges(self, entity_model: StreamlitEntityModel):
        """Make the app's grants match ``snowflake.yml``.

        ``grants is None`` leaves them unmanaged. An empty list revokes every
        non-ownership grant. Dropping only ``with_grant_option`` revokes the
        grant option and leaves the privilege. ``SHOW GRANTS`` is read before
        any revoke or grant, so a failure changes nothing.
        """
        if entity_model.grants is None:
            return
        # Read first. Treating a SHOW GRANTS failure as "no current grants"
        # would make ``grants: []`` look synced while every old grant stays.
        current = self._current_object_grants(entity_model)
        desired_by_key = {_grant_key(grant): grant for grant in entity_model.grants}
        for grant in current:
            wanted = desired_by_key.get(_grant_key(grant))
            if wanted is None:
                self.execute_query(grant.get_revoke_sql(entity_model))
            elif grant.with_grant_option and not wanted.with_grant_option:
                self.execute_query(grant.get_revoke_grant_option_sql(entity_model))
        for grant in entity_model.grants:
            self.execute_query(grant.get_grant_sql(entity_model))

    def _current_object_grants(self, entity_model: StreamlitEntityModel) -> list[Grant]:
        try:
            rows = self.execute_query(
                f"SHOW GRANTS ON STREAMLIT {entity_model.fqn.sql_identifier}",
                cursor_class=DictCursor,
            ).fetchall()
        except ProgrammingError as error:
            raise CliError(
                f"Could not list the grants on Streamlit {entity_model.fqn}, so "
                "grants removed from snowflake.yml were not revoked: "
                f"{sanitize_for_terminal(str(error.msg))}. Check that the current "
                "role can run SHOW GRANTS on the app, or remove grants from "
                "snowflake.yml to leave the app's grants unmanaged."
            ) from error

        grants: list[Grant] = []
        for row in rows:
            normalized = {str(key).lower(): value for key, value in dict(row).items()}
            privilege = str(normalized.get("privilege") or "")
            if privilege.upper() == "OWNERSHIP":
                continue
            granted_to = str(normalized.get("granted_to") or "").upper()
            grantee = normalized.get("grantee_name")
            if not privilege or not grantee:
                continue
            grant_option = (
                str(normalized.get("grant_option") or "").upper()
                in {
                    "TRUE",
                    "YES",
                }
                or normalized.get("grant_option") is True
            )
            try:
                if granted_to == "ROLE":
                    grants.append(
                        Grant(
                            privilege=privilege,
                            role=str(grantee),
                            with_grant_option=grant_option,
                        )
                    )
                elif granted_to == "USER":
                    grants.append(
                        Grant(
                            privilege=privilege,
                            user=str(grantee),
                            with_grant_option=grant_option,
                        )
                    )
            except ValueError:
                continue
        return grants

    def get_url(self, streamlit_name: FQN) -> str:
        fqn = streamlit_name.using_connection(self._conn)
        return make_snowsight_url(
            self._conn,
            f"/#/streamlit-apps/{fqn.url_identifier}",
        )

    def status(self, app_name: FQN) -> List[dict[str, Any]]:
        """Report the SPCS service backing a Streamlit app.

        Warehouse runtimes have no service. Container runtimes are matched
        through SHOW SERVICES ``managing_object_*`` columns, then
        ``SYSTEM$GET_SERVICE_STATUS`` is called for the container status.
        The app is not started.
        """
        app = app_name.using_connection(self._conn)
        if not app.database or not app.schema:
            raise CliError(
                "A database and schema are required. Pass a fully qualified "
                "name such as database.schema.app, or set them on the connection."
            )

        described = _describe_streamlit(self._conn, app)
        runtime_name = described.get("runtime_name")
        row = {
            "name": str(app),
            "runtime_name": runtime_name,
            "compute_pool": described.get("compute_pool"),
            "url": self.get_url(app),
            "service_name": None,
            "service_status": None,
            "container_status": None,
            "current_instances": None,
            "message": None,
        }
        if not is_spcs_container_runtime(runtime_name):
            row["service_status"] = _SERVICE_NOT_APPLICABLE
            row["message"] = _WAREHOUSE_MESSAGE
            return [row]

        services = _services_for_streamlit(self._conn, app)
        if not services:
            row["service_status"] = _SERVICE_NOT_STARTED
            row["message"] = _NOT_STARTED_MESSAGE
            return [row]

        results = []
        for service in services:
            service_row = dict(row)
            service_name = service.get("name")
            service_row["service_name"] = service_name
            service_row["service_status"] = service.get("status")
            service_row["current_instances"] = service.get("current_instances")
            container_status, note = self._container_status(
                _qualified_service_name(app, service_name)
            )
            service_row["container_status"] = container_status
            service_row["message"] = note
            results.append(service_row)
        return results

    def _container_status(
        self, service_name: Optional[str]
    ) -> Tuple[Optional[str], Optional[str]]:
        if not service_name:
            return None, None
        try:
            cursor = self.execute_query(
                "CALL SYSTEM$GET_SERVICE_STATUS("
                f"{to_string_literal(str(service_name))})"
            )
        except ProgrammingError as exc:
            if _is_monitor_denied(exc):
                return None, _MONITOR_MESSAGE
            raise
        try:
            fetched = cursor.fetchone()
        finally:
            cursor.close()
        if not fetched:
            return None, None
        return _summarize_service_status(fetched[0]), None


def _grant_key(grant: Grant) -> tuple[str, str, str]:
    kind = "ROLE" if grant.role else "USER"
    name = unquote_identifier(grant.role or grant.user or "")
    return (grant.privilege.upper(), kind, name.upper())


def _describe_streamlit(conn, app: FQN) -> dict[str, Any]:
    cursor = ObjectManager(connection=conn).describe(
        object_type=ObjectType.STREAMLIT.value.cli_name,
        fqn=app,
    )
    described = _one_row(cursor)
    if not described:
        raise CliError(
            f"Could not describe Streamlit app {app}. "
            "Verify the app exists and you have access."
        )
    return described


def _qualified_service_name(app: FQN, service_name: Optional[str]) -> Optional[str]:
    """Qualify the SHOW name so status resolves in the app schema, not the session schema."""
    if not service_name:
        return None
    return ".".join(
        to_identifier(part)
        for part in (app.database or "", app.schema or "", str(service_name))
    )


def _services_for_streamlit(conn, app: FQN) -> list[dict[str, Any]]:
    schema_name = f"{to_identifier(app.database)}.{to_identifier(app.schema)}"
    cursor = ObjectManager(connection=conn).show(
        object_type=ObjectType.SERVICE.value.cli_name,
        like=_SERVICE_NAME_LIKE,
        scope=("schema", f"IDENTIFIER({to_string_literal(schema_name)})"),
    )
    return [
        row
        for row in _all_rows(cursor)
        if _row_value(row, "managing_object_domain").upper() == _MANAGING_OBJECT_DOMAIN
        and _names_the_app(_row_value(row, "managing_object_name"), app)
    ]


def _one_row(cursor) -> dict[str, Any]:
    try:
        description = cursor.description or []
        row = cursor.fetchone()
        if not row or not description:
            return {}
        return {desc[0].lower(): value for desc, value in zip(description, row)}
    finally:
        cursor.close()


def _all_rows(cursor) -> list[dict[str, Any]]:
    try:
        description = cursor.description or []
        names = [desc[0].lower() for desc in description]
        return [
            {name: value for name, value in zip(names, row)}
            for row in cursor.fetchall()
        ]
    finally:
        cursor.close()


def _row_value(row: dict[str, Any], key: str) -> str:
    value = row.get(key)
    return "" if value is None else str(value).strip()


def _names_the_app(managing_name: str, app: FQN) -> bool:
    """Match SHOW managing_object_name to the app without folding quoted case."""
    if not managing_name:
        return False
    shown = [_identifier_key(part) for part in _split_qualified(managing_name)]
    expected = [
        _identifier_key(app.database or ""),
        _identifier_key(app.schema or ""),
        _identifier_key(app.name),
    ]
    if len(shown) == 3:
        return shown == expected
    if len(shown) == 2:
        return shown == expected[1:]
    if len(shown) == 1:
        return shown[0] == expected[2]
    return False


def _identifier_key(identifier: str) -> str:
    if not identifier:
        return ""
    return unquote_identifier(identifier)


def _split_qualified(name: str) -> list[str]:
    """Split a qualified name on dots that are outside double quotes."""
    parts: list[str] = []
    buffer: list[str] = []
    in_quotes = False
    index = 0
    while index < len(name):
        character = name[index]
        if character == '"':
            if in_quotes and index + 1 < len(name) and name[index + 1] == '"':
                buffer.append('""')
                index += 2
                continue
            in_quotes = not in_quotes
            buffer.append(character)
        elif character == "." and not in_quotes:
            parts.append("".join(buffer))
            buffer = []
        else:
            buffer.append(character)
        index += 1
    parts.append("".join(buffer))
    return parts


def _is_monitor_denied(exc: ProgrammingError) -> bool:
    if getattr(exc, "errno", None) == INSUFFICIENT_PRIVILEGES:
        return True
    return "insufficient privileges" in str(exc).lower()


# SHOW SERVICE CONTAINERS instance_status is READY only when every container
# in the instance is ready. Prefer a non-ready status when the containers disagree
# so an equality check against READY does not pass for a mixed payload.
_NON_READY_STATUS_PRIORITY = ("FAILED", "UNKNOWN", "PENDING", "DONE")


def _summarize_service_status(payload: Any) -> Optional[str]:
    parsed = _parse_status_payload(payload)
    if isinstance(parsed, dict):
        parsed = [parsed]
    if isinstance(parsed, list):
        statuses = [
            str(item["status"])
            for item in parsed
            if isinstance(item, dict) and item.get("status")
        ]
        rolled = _rollup_container_statuses(statuses)
        if rolled is not None:
            return rolled
    if isinstance(parsed, str) and parsed:
        return parsed
    return None


def _rollup_container_statuses(statuses: list[str]) -> Optional[str]:
    if not statuses:
        return None
    unique = list(dict.fromkeys(statuses))
    if len(unique) == 1:
        return unique[0]
    if all(status.upper() == "READY" for status in unique):
        return "READY"
    non_ready = [status for status in unique if status.upper() != "READY"]
    by_upper = {status.upper(): status for status in non_ready}
    for preferred in _NON_READY_STATUS_PRIORITY:
        if preferred in by_upper:
            return by_upper[preferred]
    return non_ready[0]


def _parse_status_payload(payload: Any) -> Any:
    if not isinstance(payload, str):
        return payload
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        return payload
