from pathlib import Path
from strictyaml import Map, Seq, Optional, UniqueSeq, Bool, Int, Str, Regex, YAMLValidationError, load
from collections import OrderedDict

IDENTIFIER = "[A-Za-z0-9_]+"
SCHEMA_AND_NAME = f"{IDENTIFIER}.{IDENTIFIER}"

# TODO: turn into regexes
Path = Str
Glob = Str

PathMapping = Map({
    "src": Glob() | Seq(Glob()),
    Optional("dest"): Path(),
})

app_schema = Map({
    "name": Str(),
    Optional("deploy_root", default="output/deploy/"): Path(),
    Optional("source_stage", default="app_src.stage"): Regex(SCHEMA_AND_NAME),
    "scripts": Map({
        Optional("package", default="package/*.sql"): Glob() | UniqueSeq(Glob()),
    }),
    "artifacts": Seq(Path() | PathMapping)
})

project_schema = Map({
    "native_app": app_schema,
})

local_schema = Map({
    "package": Map({
        "role": Regex(IDENTIFIER),
        "name": Regex(IDENTIFIER),
    }),
    "application": Map({
        "role": Regex(IDENTIFIER),
        "name": Regex(IDENTIFIER),
        Optional("warehouse"): Regex(IDENTIFIER),
        Optional("debug", default=True): Bool(),
    }),
})

def load_project_config(path: Path) -> OrderedDict:
    with open(path, 'r') as project_yml:
        return load(project_yml.read(), project_schema)

def load_local_config(path: Path) -> OrderedDict:
    with open(path, 'r') as local_yml:
        return load(local_yml.read(), local_schema)
