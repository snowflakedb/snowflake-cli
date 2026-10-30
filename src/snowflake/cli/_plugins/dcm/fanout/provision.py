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
"""Making sure every target's DCM project object exists before the run starts.

Hand-creating one DCM project per tenant does not scale, so a fan-out run creates
the missing ones itself. Existing projects are reused, missing ones are created,
and there is no opt-out.

The existence check is batched. One ``SHOW DCM PROJECTS IN SCHEMA`` per distinct
schema answers for every target in that schema, where a per-tenant check would add
one round trip per tenant.

This step runs in-process rather than as one child per tenant, for the same
reason: a thousand child processes to create a handful of objects would cost far
more than the two statements it actually takes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from snowflake.cli._plugins.dcm.manager import DCMProjectManager
from snowflake.cli._plugins.dcm.models import DCMTarget
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import same_identifiers, unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.connector import ProgrammingError

log = logging.getLogger(__name__)

# Snowflake error codes for the two failures worth naming specifically here.
# Both 2003 and 2043 report a missing or invisible object; SHOW ... IN SCHEMA
# against a schema that does not exist returns 2043.
_OBJECT_DOES_NOT_EXIST = (2003, 2043)
_INSUFFICIENT_PRIVILEGES = 3001


@dataclass
class ProvisionResult:
    """What the auto-create step found and what it did."""

    created: List[str] = field(default_factory=list)
    existing: List[str] = field(default_factory=list)
    project_identifiers: Dict[str, FQN] = field(default_factory=dict)

    @property
    def created_count(self) -> int:
        return len(self.created)


def resolve_project_identifiers(
    targets: List[DCMTarget], manager: DCMProjectManager
) -> Dict[str, FQN]:
    """Map each target name to the fully qualified project it deploys into.

    An unqualified ``project_name`` in the manifest picks up the connection's
    database and schema, which is what the single-tenant path does too.
    """
    identifiers: Dict[str, FQN] = {}
    for target in targets:
        identifier = FQN.from_string(target.project_name).using_connection(
            manager.connection
        )
        if not identifier.database or not identifier.schema:
            raise CliError(
                f"Target '{sanitize_for_terminal(target.name)}' resolves to project "
                f"'{sanitize_for_terminal(target.project_name)}', which is not fully "
                f"qualified and cannot be completed from the connection. Qualify "
                f"project_name in manifest.yml as DATABASE.SCHEMA.PROJECT, or set a "
                f"database and schema on the connection."
            )
        identifiers[target.name] = identifier
    return identifiers


def validate_project_owner(
    targets: List[DCMTarget], current_role: Optional[str]
) -> None:
    """Refuse to create anything under a role the manifest did not nominate.

    The role that creates a DCM project receives OWNERSHIP of it, so creating
    under the wrong role silently changes who can deploy the project afterwards.
    The single-tenant ``create`` command validates this for the same reason.
    """
    if not current_role:
        raise CliError(
            "The current session role could not be determined, so it cannot be checked "
            "against the manifest's project_owner. Fan-out creates DCM project objects "
            "and the creating role receives OWNERSHIP of them, so the check is required."
        )

    mismatched: List[Tuple[str, str]] = [
        (target.name, target.project_owner)
        for target in targets
        if target.project_owner
        and not same_identifiers(current_role, target.project_owner)
    ]
    if mismatched:
        names = ", ".join(sorted(name for name, _ in mismatched))
        owners = ", ".join(sorted({owner for _, owner in mismatched}))
        raise CliError(
            f"The current session role '{sanitize_for_terminal(current_role)}' does not "
            f"match the project_owner of {len(mismatched)} target(s): "
            f"{sanitize_for_terminal(names)} (project_owner: {sanitize_for_terminal(owners)}). "
            f"The role that creates a DCM project receives OWNERSHIP of it, so fan-out "
            f"will not create projects under a different role. Switch to the expected role, "
            f"or correct project_owner in manifest.yml."
        )

    undeclared = [target.name for target in targets if not target.project_owner]
    if undeclared:
        log.info(
            "Cannot validate project_owner for %d target(s); the manifest does not "
            "declare one.",
            len(undeclared),
        )


def _existing_project_names(
    manager: DCMProjectManager, database: str, schema: str
) -> set:
    """The set of DCM projects already in one schema, as unquoted upper names."""
    try:
        cursor = manager.list_projects_in_schema(database=database, schema=schema)
    except ProgrammingError as error:
        raise _container_error(error, database, schema)

    names = set()
    name_column = _name_column_index(cursor)
    for row in cursor.fetchall():
        if name_column is None or name_column >= len(row):
            continue
        value = row[name_column]
        if value:
            names.add(unquote_identifier(str(value)))
    return names


def _name_column_index(cursor) -> Optional[int]:
    """Locate the ``name`` column rather than assuming its position."""
    description = getattr(cursor, "description", None) or []
    for index, column in enumerate(description):
        column_name = getattr(column, "name", None)
        if column_name is None and isinstance(column, (list, tuple)) and column:
            column_name = column[0]
        if column_name and str(column_name).lower() == "name":
            return index
    return 1 if description else None


def _container_error(error: ProgrammingError, database: str, schema: str) -> CliError:
    """Turn a SHOW failure into an error that names the schema or the privilege.

    At a thousand tenants a generic failure is unactionable, so the two failures
    that actually happen here are called out by name.
    """
    location = f"{sanitize_for_terminal(database)}.{sanitize_for_terminal(schema)}"
    if error.errno in _OBJECT_DOES_NOT_EXIST:
        return CliError(
            f"Schema {location} does not exist or is not visible to the current role. "
            f"A DCM project cannot create its own parent database or schema, so create "
            f"{location} first, then re-run."
        )
    if error.errno == _INSUFFICIENT_PRIVILEGES:
        return CliError(
            f"The current role cannot list DCM projects in {location}. Creating a DCM "
            f"project there requires CREATE DCM PROJECT ON SCHEMA {location}. Grant it, "
            f"or switch to a role that already has it."
        )
    return CliError(
        f"Could not list DCM projects in {location}: "
        f"{sanitize_for_terminal(str(error))}"
    )


def _create_error(error: ProgrammingError, identifier: FQN) -> CliError:
    location = f"{sanitize_for_terminal(str(identifier.database))}.{sanitize_for_terminal(str(identifier.schema))}"
    if error.errno == _INSUFFICIENT_PRIVILEGES:
        return CliError(
            f"The current role cannot create DCM project "
            f"'{sanitize_for_terminal(str(identifier))}'. This requires "
            f"CREATE DCM PROJECT ON SCHEMA {location}. Grant it, or switch to a role "
            f"that already has it."
        )
    if error.errno in _OBJECT_DOES_NOT_EXIST:
        return CliError(
            f"Cannot create DCM project '{sanitize_for_terminal(str(identifier))}' "
            f"because schema {location} does not exist. A DCM project cannot create its "
            f"own parent database or schema, so create {location} first, then re-run."
        )
    return CliError(
        f"Could not create DCM project '{sanitize_for_terminal(str(identifier))}': "
        f"{sanitize_for_terminal(str(error))}"
    )


def plan_provisioning(
    targets: List[DCMTarget],
    manager: DCMProjectManager,
) -> ProvisionResult:
    """Work out which project objects exist and which are missing, creating nothing."""
    identifiers = resolve_project_identifiers(targets, manager)

    by_schema: Dict[Tuple[str, str], List[str]] = {}
    for name, identifier in identifiers.items():
        key = (str(identifier.database), str(identifier.schema))
        by_schema.setdefault(key, []).append(name)

    result = ProvisionResult(project_identifiers=identifiers)
    for (database, schema), target_names in sorted(by_schema.items()):
        existing = _existing_project_names(manager, database, schema)
        log.info(
            "Checked DCM projects in one schema (targets=%d, existing_in_schema=%d).",
            len(target_names),
            len(existing),
        )
        for name in sorted(target_names):
            identifier = identifiers[name]
            if unquote_identifier(identifier.name) in existing:
                result.existing.append(str(identifier))
            else:
                result.created.append(str(identifier))
    return result


def ensure_projects(
    targets: List[DCMTarget],
    manager: DCMProjectManager,
    current_role: Optional[str],
) -> ProvisionResult:
    """Create every missing project object, after validating the executing role."""
    validate_project_owner(targets, current_role)
    planned = plan_provisioning(targets, manager)

    created: List[str] = []
    for identifier_text in planned.created:
        identifier = FQN.from_string(identifier_text)
        try:
            manager.create(project_identifier=identifier)
        except ProgrammingError as error:
            raise _create_error(error, identifier)
        created.append(identifier_text)

    log.info(
        "Ensured DCM project objects exist (created=%d, existing=%d).",
        len(created),
        len(planned.existing),
    )
    return ProvisionResult(
        created=created,
        existing=planned.existing,
        project_identifiers=planned.project_identifiers,
    )
