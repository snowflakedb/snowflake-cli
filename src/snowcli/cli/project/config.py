import os
import re
from pathlib import Path
from strictyaml import (
    MapCombined,
    Seq,
    Optional,
    UniqueSeq,
    Bool,
    Str,
    Any,
    Regex,
    load,
    as_document,
)
from collections import OrderedDict

IDENTIFIER = r'(?:("[^"]*(""[^"]*)*")|([A-Za-z_][\w$]{0,254}))'
SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}"
GLOB_REGEX = r"^[a-zA-Z0-9_\-./*?**\p{L}\p{N}]+$"
RELATIVE_PATH = r"^[^/][\p{L}\p{N}_\-.][^/]*$"

# TODO: use the above regexes to validate paths + globs
Path = Str
Glob = Str


class RelaxedMap(MapCombined):
    """A version of a Map that allows any number of unknown key/value pairs."""

    def __init__(self, map_validator):
        super(RelaxedMap, self).__init__(map_validator, Str(), Bool() | Any())


PathMapping = RelaxedMap(
    {
        "src": Glob() | Seq(Glob()),
        Optional("dest"): Path(),
    }
)

app_schema = RelaxedMap(
    {
        "name": Str(),
        Optional("deploy_root", default="output/deploy/"): Path(),
        Optional("source_stage", default="app_src.stage"): Regex(SCHEMA_AND_NAME),
        Optional("scripts", default=OrderedDict(package="package/*.sql")): RelaxedMap(
            {
                Optional("package", default="package/*.sql"): Glob()
                | UniqueSeq(Glob()),
            }
        ),
        "artifacts": Seq(Path() | PathMapping),
    }
)

local_schema = RelaxedMap(
    {
        "native_app": RelaxedMap(
            {
                "package": RelaxedMap(
                    {
                        "role": Regex(IDENTIFIER),
                        "name": Regex(IDENTIFIER),
                    }
                ),
                "application": RelaxedMap(
                    {
                        "role": Regex(IDENTIFIER),
                        "name": Regex(IDENTIFIER),
                        Optional("warehouse"): Regex(IDENTIFIER),
                        Optional("debug", default=True): Bool(),
                    }
                ),
            }
        ),
    }
)

project_schema = RelaxedMap(
    {
        "native_app": app_schema,
    }
)


def load_project_config(path: Path) -> OrderedDict:
    with open(path, "r") as project_yml:
        return load(project_yml.read(), project_schema)


def load_local_config(path: Path) -> OrderedDict:
    with open(path, "r") as local_yml:
        return load(local_yml.read(), local_schema)


def clean_identifier(input):
    """
    Removes characters that cannot be used in an unquoted identifier,
    converting to lowercase as well.
    """
    return re.sub(r"[^a-z0-9_$]", "", f"{input}".lower())


def generate_local_config(project: OrderedDict, conn: dict) -> OrderedDict:
    user = clean_identifier(os.getenv("USER"))
    role = conn.get("role", "accountadmin")

    local = OrderedDict()
    if "native_app" in project:
        local["native_app"] = OrderedDict()

        name = clean_identifier(project["native_app"]["name"])

        local["native_app"]["application"] = OrderedDict()
        local["native_app"]["application"]["name"] = f"{name}_{user}"
        local["native_app"]["application"]["role"] = role

        local["native_app"]["package"] = OrderedDict()
        local["native_app"]["package"]["name"] = f"{name}_pkg_{user}"
        local["native_app"]["package"]["role"] = role

    return as_document(local)
