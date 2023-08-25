import os
from pathlib import Path
from strictyaml import (
    Map,
    Seq,
    Optional,
    UniqueSeq,
    Bool,
    Str,
    Regex,
    load,
    as_document,
)
from collections import OrderedDict

IDENTIFIER = "(.+)"  # FIXME: actual identifier regex? https://docs.snowflake.com/en/sql-reference/identifiers-syntax
SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}"

# TODO: turn into regexes
Path = Str
Glob = Str

PathMapping = Map(
    {
        "src": Glob() | Seq(Glob()),
        Optional("dest"): Path(),
    }
)

app_schema = Map(
    {
        "name": Str(),
        Optional("deploy_root", default="output/deploy/"): Path(),
        Optional("source_stage", default="app_src.stage"): Regex(SCHEMA_AND_NAME),
        "scripts": Map(
            {
                Optional("package", default="package/*.sql"): Glob()
                | UniqueSeq(Glob()),
            }
        ),
        "artifacts": Seq(Path() | PathMapping),
    }
)

local_schema = Map(
    {
        "native_app": Map(
            {
                "package": Map(
                    {
                        "role": Regex(IDENTIFIER),
                        "name": Regex(IDENTIFIER),
                    }
                ),
                "application": Map(
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

project_schema = Map(
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


def generate_local_config(project: OrderedDict, conn: dict) -> OrderedDict:
    user = os.getenv("USER")
    role = conn.get("role", "accountadmin")

    local = OrderedDict()
    if "native_app" in project:
        local["native_app"] = OrderedDict()

        local["native_app"]["application"] = OrderedDict()
        local["native_app"]["application"][
            "name"
        ] = f"{project['native_app']['name']}_{user}"
        local["native_app"]["application"]["role"] = role

        local["native_app"]["package"] = OrderedDict()
        local["native_app"]["package"][
            "name"
        ] = f"{project['native_app']['name']}_pkg_{user}"
        local["native_app"]["package"]["role"] = role

    return as_document(local)
