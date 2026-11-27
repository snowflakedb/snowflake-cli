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

"""The role a Snowflake App Runtime deploy runs as.

A deploy never switches roles: every object it creates is owned by the
session's primary role, and an object that already exists keeps its owner.
A deploy under a role nobody asked for leaves objects that have to be dropped
to fix, so the role is checked before anything is created.
"""

from __future__ import annotations

import logging
from typing import NamedTuple, Optional, Sequence, Tuple

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import get_env_value
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import unquote_identifier
from snowflake.cli.api.sanitizers import sanitize_for_terminal

log = logging.getLogger(__name__)


class RoleRequest(NamedTuple):
    """A role a deploy was asked to run as, and where it was asked for."""

    role: str
    source: str
    from_entity: bool = False


class DeployRoleRequest(NamedTuple):
    """The roles requested for a deploy, and whether the session is reused.

    A connection that passes a session token and a master token reuses an
    existing session instead of signing in, so the role it requests is never
    applied: the session keeps the role it already had.
    """

    requests: Tuple[RoleRequest, ...]
    reuses_session: bool


def entity_role_request(entity_id: str, role: Optional[str]) -> Optional[RoleRequest]:
    """The ``meta.role`` a ``snowflake.yml`` entity sets, if any."""
    if not role:
        return None
    return RoleRequest(
        role=role, source=f"meta.role of entity '{entity_id}'", from_entity=True
    )


def deploy_role_request(
    entity_request: Optional[RoleRequest] = None,
) -> DeployRoleRequest:
    """Collect the roles requested for this deploy.

    The connection's role is resolved in the order the connector receives it:
    ``--role``, then the connection's configuration, then ``SNOWFLAKE_ROLE``.
    Configuration is read on a clone of the connection context, so the shared
    context, and the connection cache key derived from it, stay unchanged.
    """
    context = get_cli_context().connection_context
    resolved = context.clone()
    try:
        resolved.validate_and_complete()
        resolved.update_from_config()
    except Exception:
        log.debug("Could not read the connection configuration.", exc_info=True)
        resolved = context

    requests = [entity_request] if entity_request else []
    if context.role:
        requests.append(RoleRequest(role=context.role, source="--role"))
    elif resolved.role:
        requests.append(
            RoleRequest(
                role=resolved.role,
                source=f"connection '{resolved.connection_name}'",
            )
        )
    elif env_role := get_env_value(key="role"):
        requests.append(RoleRequest(role=env_role, source="SNOWFLAKE_ROLE"))

    session_token = resolved.session_token or get_env_value(key="session_token")
    master_token = resolved.master_token or get_env_value(key="master_token")
    return DeployRoleRequest(
        requests=tuple(requests),
        reuses_session=bool(session_token and master_token),
    )


def check_deploy_role(role: str, request: DeployRoleRequest) -> None:
    """Refuse to deploy as *role* when a different role was requested."""
    for requested in request.requests:
        if unquote_identifier(requested.role) != role:
            raise CliError(
                _role_mismatch_message(requested, role, request.reuses_session)
            )


def _role_mismatch_message(
    requested: RoleRequest, role: str, reuses_session: bool
) -> str:
    wanted = sanitize_for_terminal(unquote_identifier(requested.role))
    actual = sanitize_for_terminal(role)
    if reuses_session:
        cause = (
            "the connection reuses an existing session through a session token "
            f"and master token, and that session runs as role {actual}"
        )
        to_wanted = (
            f"To deploy as {wanted}, connect without a session token and master "
            "token" + (f" and pass --role {wanted}." if requested.from_entity else ".")
        )
    else:
        cause = f"the session runs as role {actual}"
        to_wanted = (
            f"To deploy as {wanted}, pass --role {wanted}: a Snowflake App "
            "Runtime deploy does not switch to the role in meta.role."
            if requested.from_entity
            else f"Check that the connection signs in with role {wanted}."
        )
    to_actual = (
        f"To deploy as {actual}, remove meta.role from the entity."
        if requested.from_entity
        else f"To deploy as {actual}, pass --role {actual}."
    )
    return (
        f"Role {wanted} from {requested.source} is not active: {cause}. "
        f"Objects this deploy creates would be owned by {actual}. "
        f"{to_wanted} {to_actual}"
    )


def other_owners_warning(
    role: str, owners: Sequence[Tuple[str, FQN, Optional[str]]]
) -> Optional[str]:
    """Describe the existing objects a deploy as *role* updates but does not own.

    *owners* holds ``(object kind, FQN, owner)`` for each object the deploy
    updates in place; an owner of ``None`` means the object does not exist or
    cannot be seen.
    """
    others = [
        f"the {kind} {sanitize_for_terminal(fqn.identifier)} "
        f"(owned by {sanitize_for_terminal(owner)})"
        for kind, fqn, owner in owners
        if owner and owner != role
    ]
    if not others:
        return None
    actual = sanitize_for_terminal(role)
    exist, them, they_keep = (
        ("exist", "them", "they keep their owner")
        if len(others) > 1
        else ("exists", "it", "it keeps its owner")
    )
    subject = " and ".join(others)
    return (
        f"{subject[0].upper()}{subject[1:]} already {exist}. Deploying as role "
        f"{actual} updates {them} in place, so {they_keep}. To have {actual} "
        f"own {them}, drop {them} and deploy again."
    )
