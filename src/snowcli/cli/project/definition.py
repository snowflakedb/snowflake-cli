from .util import clean_identifier, get_env_username
from pathlib import Path
from typing import List, Union, Dict
from strictyaml import (
    YAML,
    load,
    as_document,
)

from .schema import (
    project_schema,
    project_override_schema,
)

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager

DEFAULT_USERNAME = "unknown_user"


def merge_left(target: Union[Dict, YAML], source: Union[Dict, YAML]) -> None:
    """
    Recursively merges key/value pairs from source into target.
    Modifies the original dict-like "target".
    """
    for k, v in source.items():
        if k in target and (
            isinstance(v, dict) or (isinstance(v, YAML) and not v.is_scalar())
        ):
            # assumption: all inputs have been validated.
            assert isinstance(target[k], dict) or isinstance(target[k], YAML)
            merge_left(target[k], v)
        else:
            target[k] = v


def load_project_definition(paths: List[Path]) -> dict:
    """
    Loads project definition, optionally overriding values. Definition values
    are merged in left-to-right order (increasing precedence).
    """
    if len(paths) == 0:
        raise ValueError("Need at least one definition file.")

    with open(paths[0], "r") as base_yml:
        definition = load(base_yml.read(), project_schema)

    for override_path in paths[1:]:
        with open(override_path, "r") as override_yml:
            overrides = load(override_yml.read(), project_override_schema)
            merge_left(definition, overrides)

        # TODO: how to show good error messages here?
        definition.revalidate(project_schema)

    return definition.data


def generate_local_override_yml(project: Union[Dict, YAML]) -> YAML:
    """
    Generates defaults for optional keys in the same YAML structure as the project
    schema. The returned YAML object can be saved directly to a file, if desired.
    A connection is made using global context to resolve current role and warehouse.
    """
    conn = snow_cli_global_context_manager.get_connection()
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    role = conn.role
    warehouse = conn.warehouse

    local: dict = {}
    if "native_app" in project:
        name = clean_identifier(project["native_app"]["name"])
        local["native_app"] = {
            "application": {
                "name": f"{name}_{user}",
                "role": role,
                "debug": True,
                "warehouse": warehouse,
            },
            "package": {"name": f"{name}_pkg_{user}", "role": role},
        }

    return as_document(local, project_override_schema)
