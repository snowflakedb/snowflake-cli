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

import logging
import re
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from click import UsageError
from snowflake.cli._plugins.snowpark.models import Requirement
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    ProcedureEntityModel,
    SnowparkEntityModel,
)
from snowflake.cli._plugins.snowpark.snowpark_project_paths import (
    Artifact,
    SnowparkProjectPaths,
)
from snowflake.cli._plugins.snowpark.zipper import zip_dir_using_bundle_map
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.utils import symlink_or_copy
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.constants import (
    INIT_TEMPLATE_VARIABLE_CLOSING,
    INIT_TEMPLATE_VARIABLE_OPENING,
    PROJECT_TEMPLATE_VARIABLE_CLOSING,
    PROJECT_TEMPLATE_VARIABLE_OPENING,
    ObjectType,
)
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor

log = logging.getLogger(__name__)

SnowparkEntities = Dict[str, SnowparkEntityModel]
StageToArtifactMapping = Dict[str, set[Artifact]]
EntityToImportPathsMapping = Dict[str, set[str]]

DEFAULT_RUNTIME = "3.10"


class SnowparkObject(Enum):
    """This clas is used only for Snowpark execute where choice is limited."""

    PROCEDURE = str(ObjectType.PROCEDURE)
    FUNCTION = str(ObjectType.FUNCTION)


class SnowparkObjectManager(SqlExecutionMixin):
    def execute(
        self, execution_identifier: str, object_type: SnowparkObject
    ) -> SnowflakeCursor:
        if object_type == SnowparkObject.FUNCTION:
            return self.execute_query(f"select {execution_identifier}")
        if object_type == SnowparkObject.PROCEDURE:
            return self.execute_query(f"call {execution_identifier}")
        raise UsageError(f"Unknown object type: {object_type}.")

    def create_or_replace(
        self,
        entity: SnowparkEntityModel,
        artifact_files: set[str],
        snowflake_dependencies: list[str],
    ) -> str:
        entity.imports.extend(artifact_files)
        imports = [f"'{x}'" for x in entity.imports]

        object_type = entity.get_type()

        query = [
            f"create or replace {object_type} {entity.udf_sproc_identifier.identifier_for_sql}",
            f"copy grants",
            f"returns {entity.returns}",
            "language python",
            f"runtime_version={entity.runtime or DEFAULT_RUNTIME}",
            f"imports=({', '.join(imports)})",
            f"handler='{entity.handler}'",
        ]

        if entity.external_access_integrations:
            query.append(entity.get_external_access_integrations_sql())

        if entity.secrets:
            query.append(entity.get_secrets_sql())

        if entity.artifact_repository_packages and entity.packages:
            raise UsageError(
                "You cannot specify both artifact_repository_packages and packages.",
            )

        packages_list = snowflake_dependencies.copy()
        if entity.artifact_repository and (
            entity.artifact_repository_packages or entity.packages
        ):
            if entity.artifact_repository_packages:
                packages_list.extend(entity.artifact_repository_packages)
            else:
                packages_list.extend(entity.packages)
            query.append(
                f"ARTIFACT_REPOSITORY= {entity.artifact_repository}",
            )
        packages = [f"'{item}'" for item in packages_list]
        query.append(f"packages=({','.join(packages)})")

        if entity.resource_constraint:
            constraints = ",".join(
                f"{key}='{value}'" for key, value in entity.resource_constraint.items()
            )
            query.append(f"RESOURCE_CONSTRAINT=({constraints})")

        if isinstance(entity, ProcedureEntityModel) and entity.execute_as_caller:
            query.append("execute as caller")

        return self.execute_query("\n".join(query))

    def deploy_entity(
        self,
        entity: SnowparkEntityModel,
        existing_objects: Dict[str, SnowflakeCursor],
        snowflake_dependencies: List[str],
        entities_to_artifact_map: EntityToImportPathsMapping,
    ):
        cli_console.step(f"Creating {entity.type} {entity.fqn}")
        object_exists = entity.entity_id in existing_objects
        replace_object = False
        if object_exists:
            replace_object = _check_if_replace_is_required(
                entity=entity,
                current_state=existing_objects[entity.entity_id],
                snowflake_dependencies=snowflake_dependencies,
                stage_artifact_files=entities_to_artifact_map[entity.entity_id],
            )

        state = {
            "object": entity.udf_sproc_identifier.identifier_with_arg_names_types_defaults,
            "type": entity.get_type(),
        }
        if object_exists and not replace_object:
            return {**state, "status": "packages updated"}

        self.create_or_replace(
            entity=entity,
            artifact_files=entities_to_artifact_map[entity.entity_id],
            snowflake_dependencies=snowflake_dependencies,
        )
        return {
            **state,
            "status": "created" if not object_exists else "definition updated",
        }


def _check_if_replace_is_required(
    entity: SnowparkEntityModel,
    current_state,
    snowflake_dependencies: List[str],
    stage_artifact_files: set[str],
) -> bool:
    object_type = entity.get_type()
    resource_json = _convert_resource_details_to_dict(current_state)
    old_dependencies = resource_json["packages"]

    if _snowflake_dependencies_differ(old_dependencies, snowflake_dependencies):
        log.info(
            "Found difference of package requirements. Replacing the %s.", object_type
        )
        return True

    if set(entity.external_access_integrations) != set(
        resource_json.get("external_access_integrations", [])
    ):
        log.info(
            "Found difference of external access integrations. Replacing the %s.",
            object_type,
        )
        return True

    if resource_json["handler"].lower() != entity.handler.lower() or not same_type(
        resource_json["returns"], entity.returns
    ):
        log.info(
            "Return type or handler types do not match. Replacing the %s.", object_type
        )
        return True

    if "signature" in resource_json and _signatures_differ(
        resource_json["signature"] or "", entity
    ):
        log.info("Argument signature does not match. Replacing the %s.", object_type)
        return True

    if _compare_imports(resource_json, entity.imports, stage_artifact_files):
        log.info("Imports do not match. Replacing the %s", object_type)
        return True

    if entity.runtime is not None and entity.runtime != resource_json.get(
        "runtime_version", "RUNTIME_NOT_SET"
    ):
        log.info("Runtime versions do not match. Replacing the %s", object_type)
        return True

    if entity.resource_constraint != resource_json.get("resource_constraint", None):
        log.info("Resource constraints do not match. Replacing the %s", object_type)
        return True

    if entity.artifact_repository != resource_json.get("artifact_repository", None):
        log.info("Artifact repository does not match. Replacing the %s", object_type)
        return True

    if entity.artifact_repository_packages != resource_json.get(
        "artifact_repository_packages", None
    ):
        log.info(
            "Artifact repository packages do not match. Replacing the %s", object_type
        )
        return True

    if entity.packages != resource_json.get("artifact_repository_packages", None):
        log.info("Packages do not match. Replacing the %s", object_type)
        return True

    if isinstance(entity, ProcedureEntityModel):
        if resource_json.get("execute as", "OWNER") != (
            "CALLER" if entity.execute_as_caller else "OWNER"
        ):
            log.info(
                "Execute as caller settings do not match. Replacing the %s", object_type
            )
            return True

    return False


def _convert_resource_details_to_dict(function_details: SnowflakeCursor) -> dict:
    import json

    function_dict = {}
    json_properties = ["packages", "installed_packages"]
    for function in function_details:
        if function[0] in json_properties:
            function_dict[function[0]] = json.loads(
                function[1].replace("'", '"'),
            )
        else:
            function_dict[function[0]] = function[1]
    return function_dict


def _snowflake_dependencies_differ(
    old_dependencies: List[str], new_dependencies: List[str]
) -> bool:
    def _standardize(packages: List[str]) -> Set[str]:
        return set(
            Requirement.parse_line(package).name_and_version for package in packages
        )

    return _standardize(old_dependencies) != _standardize(new_dependencies)


def map_path_mapping_to_artifact(
    project_paths: SnowparkProjectPaths, artifacts: List[PathMapping]
) -> List[Artifact]:
    return [project_paths.get_artifact_dto(artifact) for artifact in artifacts]


def zip_and_copy_artifacts_to_deploy(
    artifacts: Set[Artifact] | List[Artifact], bundle_root: Path
) -> List[Path]:
    copied_files = []
    for artifact in artifacts:
        bundle_map = BundleMap(
            project_root=artifact.project_root,
            deploy_root=bundle_root,
        )
        bundle_map.add(PathMapping(src=str(artifact.path), dest=artifact.dest))

        if artifact.path.is_file():
            for (absolute_src, absolute_dest) in bundle_map.all_mappings(
                absolute=True, expand_directories=False
            ):
                symlink_or_copy(
                    absolute_src,
                    absolute_dest,
                    deploy_root=bundle_map.deploy_root(),
                )
                copied_files.append(absolute_dest)
        else:
            post_build_path = artifact.post_build_path
            zip_dir_using_bundle_map(
                bundle_map=bundle_map,
                dest_zip=post_build_path,
            )
            copied_files.append(post_build_path)
    return copied_files


def same_type(sf_type: str, local_type: str) -> bool:
    sf_type, local_type = sf_type.upper(), local_type.upper()

    # 1. Types are equal out of the box
    if sf_type == local_type:
        return True

    # 2. Local type is alias for Snowflake type
    local_type = user_to_sql_type_mapper(local_type).upper()
    if sf_type == local_type:
        return True

    # 3. Local type is a subset of Snowflake type, e.g. VARCHAR(N) == VARCHAR
    # We solved for local VARCHAR(N) in point 1 & 2 as those are explicit types
    if sf_type.startswith(local_type):
        return True

    # 4. Snowflake types is subset of local type
    if local_type.startswith(sf_type):
        return True

    return False


def user_to_sql_type_mapper(user_provided_type: str) -> str:
    mapping = {
        ("VARCHAR", "(16777216)"): ("CHAR", "TEXT", "STRING"),
        ("BINARY", "(8388608)"): ("BINARY", "VARBINARY"),
        ("NUMBER", "(38,0)"): (
            "NUMBER",
            "DECIMAL",
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
        ),
        ("FLOAT", ""): (
            "FLOAT",
            "DOUBLE",
            "DOUBLE PRECISION",
            "REAL",
            "FLOAT",
            "FLOAT4",
            "FLOAT8",
        ),
        ("DECFLOAT", ""): ("DECFLOAT",),
        ("TIMESTAMP_NTZ", ""): ("TIMESTAMP_NTZ", "TIMESTAMPNTZ", "DATETIME"),
        ("TIMESTAMP_LTZ", ""): ("TIMESTAMP_LTZ", "TIMESTAMPLTZ"),
        ("TIMESTAMP_TZ", ""): ("TIMESTAMP_TZ", "TIMESTAMPTZ"),
    }

    user_provided_type = user_provided_type.upper()
    for (cast_type, default), matching_types in mapping.items():
        for type_ in matching_types:
            if user_provided_type == type_:
                # TEXT -> VARCHAR(16777216)
                return cast_type + default
            if user_provided_type.startswith(type_):
                # TEXT(30) -> VARCHAR(30)
                return user_provided_type.replace(type_, cast_type + default)
    return user_provided_type


def _signatures_differ(remote_signature: str, entity: SnowparkEntityModel) -> bool:
    """Return True if the remote DESCRIBE signature does not match the signature
    declared locally on ``entity``. Detects changes to argument names, types, and
    defaults — including adding or removing a default on an existing argument."""
    remote_args = _parse_remote_signature(remote_signature)
    local_args = _local_signature_args(entity)

    if len(remote_args) != len(local_args):
        return True

    for (r_name, r_type, r_default), (l_name, l_type, l_default) in zip(
        remote_args, local_args
    ):
        if r_name.lower() != l_name.lower():
            return True
        if not same_type(r_type, l_type):
            return True
        if _normalize_default(r_default) != _normalize_default(l_default):
            return True
    return False


def _parse_remote_signature(
    signature: str,
) -> List[Tuple[str, str, Optional[str]]]:
    """Parse a signature string returned by DESCRIBE PROCEDURE/FUNCTION into a
    list of ``(name, type, default)`` tuples. Returns an empty list for ``()``.

    The Snowflake DESCRIBE output looks like ``(NAME VARCHAR, AGE NUMBER DEFAULT 10)``.
    """
    if not signature:
        return []
    stripped = signature.strip()
    if stripped.startswith("(") and stripped.endswith(")"):
        stripped = stripped[1:-1].strip()
    if not stripped:
        return []

    args: List[Tuple[str, str, Optional[str]]] = []
    for raw_arg in _split_signature_args(stripped):
        name, _, rest = raw_arg.strip().partition(" ")
        rest = rest.strip()
        if not rest:
            # Defensive: signature entry without a type is not something we can
            # reason about. Treat as having no default.
            args.append((name, "", None))
            continue
        upper_rest = rest.upper()
        default_idx = upper_rest.find(" DEFAULT ")
        if default_idx == -1:
            args.append((name, rest, None))
        else:
            type_part = rest[:default_idx].strip()
            default_part = rest[default_idx + len(" DEFAULT ") :].strip()
            args.append((name, type_part, default_part))
    return args


def _split_signature_args(body: str) -> List[str]:
    """Split a signature body on commas while respecting single-quoted strings
    and parentheses (e.g. ``NUMBER(38,0)``)."""
    parts = []
    buf = []
    depth = 0
    in_quote = False
    i = 0
    while i < len(body):
        ch = body[i]
        if in_quote:
            buf.append(ch)
            if ch == "'":
                # Handle doubled single-quote escape: ''
                if i + 1 < len(body) and body[i + 1] == "'":
                    buf.append(body[i + 1])
                    i += 2
                    continue
                in_quote = False
        else:
            if ch == "'":
                buf.append(ch)
                in_quote = True
            elif ch == "(":
                depth += 1
                buf.append(ch)
            elif ch == ")":
                depth -= 1
                buf.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _local_signature_args(
    entity: SnowparkEntityModel,
) -> List[Tuple[str, str, Optional[str]]]:
    """Return the local entity's arguments as ``(name, type, default)`` tuples
    with string defaults quoted the same way they are rendered into SQL."""
    signature = entity.signature
    if isinstance(signature, str):
        return _parse_remote_signature(signature)
    if not signature or signature == "null":
        return []

    args: List[Tuple[str, str, Optional[str]]] = []
    for arg in signature:
        name = arg.name
        _type = arg.arg_type
        _default = arg.default
        rendered_default = _default
        if (
            rendered_default is not None
            and _type.lower() in ("string", "varchar")
            and rendered_default.lower() != "null"
        ):
            rendered_default = f"'{rendered_default}'"
        args.append((name, _type, rendered_default))
    return args


def _normalize_default(default: str | None) -> str | None:
    """Normalize a default expression so that differently-rendered-but-equivalent
    defaults compare equal (trimmed whitespace, case-insensitive for identifiers
    and keywords, but quoted string contents are preserved verbatim)."""
    if default is None:
        return None
    value = default.strip()
    if not value:
        return None
    if value.startswith("'") and value.endswith("'") and len(value) >= 2:
        # Preserve string literal contents (case-sensitive).
        return value
    return value.upper()


def _compare_imports(
    resource_json: dict, imports: List[str], artifact_files: set[str]
) -> bool:
    pattern = re.compile(r"(?:\[@?\w+_\w+\.)?(\w+(?:/\w+)+\.\w+)(?:\])?")

    project_imports = {
        imp
        for import_string in [*imports, *artifact_files]
        for imp in pattern.findall(import_string.lower())
    }

    if "imports" not in resource_json.keys():
        object_imports = set()
    else:
        object_imports = {
            imp.lower()
            for imp in pattern.findall(resource_json.get("imports", "").lower())
        }

    return project_imports != object_imports


def is_name_a_templated_one(name: str) -> bool:
    return (
        PROJECT_TEMPLATE_VARIABLE_OPENING in name
        and PROJECT_TEMPLATE_VARIABLE_CLOSING in name
    ) or (
        INIT_TEMPLATE_VARIABLE_OPENING in name
        and INIT_TEMPLATE_VARIABLE_CLOSING in name
    )
