from .util import clean_identifier, get_env_username
from pathlib import Path
from typing import List
from strictyaml import (
    YAML,
    load,
    as_document,
)

from .schema import (
    project_schema,
    project_override_schema,
)


DEFAULT_USERNAME = "unknown_user"


def merge_left(target: dict | YAML, source: dict | YAML) -> None:
    """
    Recursively merges key/value pairs from source into target.
    Modifies the original dict-like "target".
    """
    for k, v in source.items():
        if k in target and (isinstance(v, dict) or isinstance(v, YAML)):
            # assumption: all inputs have been validated.
            assert isinstance(target[k], dict) or isinstance(target[k], YAML)
            merge_left(target[k], v)
        else:
            target[k] = v


def load_project_config(paths: List[Path]) -> dict:
    """
    Loads a project config, optionally overriding values. Configuration is merged
    in order of left to right (increasing precedence).
    """
    if len(paths) == 0:
        raise ValueError("Need at least one configuration file.")

    with open(paths[0], "r") as base_yml:
        config = load(base_yml.read(), project_schema)

    for override_path in paths[1:]:
        with open(override_path, "r") as override_yml:
            overrides = load(override_yml.read(), project_override_schema)
            merge_left(config, overrides)

        # TODO: how to show good error messages here?
        config.revalidate(project_schema)

    return config.data


def generate_local_override_yml(project: dict | YAML, conn: dict) -> YAML:
    user = clean_identifier(get_env_username() or DEFAULT_USERNAME)
    role = conn.get("role", "accountadmin")  # TODO: actual connection

    local: dict = {}
    if "native_app" in project:
        name = clean_identifier(project["native_app"]["name"])
        local["native_app"] = {
            "application": {
                "name": f"{name}_{user}",
                "role": role,
                "debug": True,
                # TODO: warehouse from actual connection
            },
            "package": {"name": f"{name}_pkg_{user}", "role": role},
        }

    return as_document(local, project_override_schema)
