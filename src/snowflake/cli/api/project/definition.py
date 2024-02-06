from pathlib import Path
from typing import Dict, List, Union

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.schemas.project_definition import (
    project_override_schema,
    project_schema,
)
from snowflake.cli.api.project.util import (
    append_to_identifier,
    clean_identifier,
    get_env_username,
    to_identifier,
)
from strictyaml import (
    YAML,
    as_document,
    load,
)

DEFAULT_USERNAME = "unknown_user"


def merge_left(target: Union[Dict, YAML], source: Union[Dict, YAML]) -> None:
    """
    Recursively merges key/value pairs from source into target.
    Modifies the original dict-like "target".
    """
    for k, v in source.items():
        if k in target and (
            isinstance(v, dict) or (isinstance(v, YAML) and v.is_mapping())
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
    conn = cli_context.connection
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    role = conn.role
    warehouse = conn.warehouse

    local: dict = {}
    if "native_app" in project:
        name = clean_identifier(project["native_app"]["name"])
        app_identifier = to_identifier(name)
        user_app_identifier = append_to_identifier(app_identifier, f"_{user}")
        package_identifier = append_to_identifier(app_identifier, f"_pkg_{user}")
        local["native_app"] = {
            "application": {
                "name": user_app_identifier,
                "role": role,
                "debug": True,
                "warehouse": warehouse,
            },
            "package": {"name": package_identifier, "role": role},
        }

    return as_document(local, project_override_schema)


def default_app_package(project_name: str):
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    return append_to_identifier(to_identifier(project_name), f"_pkg_{user}")


def default_role():
    conn = cli_context.connection
    return conn.role


def default_application(project_name: str):
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    return append_to_identifier(to_identifier(project_name), f"_{user}")
