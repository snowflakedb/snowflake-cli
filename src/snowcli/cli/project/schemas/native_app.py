from __future__ import annotations

from snowcli.cli.project.schemas.relaxed_map import FilePath, Glob, RelaxedMap
from snowcli.cli.project.util import IDENTIFIER, SCHEMA_AND_NAME
from strictyaml import (
    Bool,
    Optional,
    Regex,
    Seq,
    Str,
    UniqueSeq,
)

PathMapping = RelaxedMap(
    {
        "src": Glob() | Seq(Glob()),
        Optional("dest"): FilePath(),
    }
)

native_app_schema = RelaxedMap(
    {
        "name": Str(),
        "artifacts": Seq(FilePath() | PathMapping),
        Optional("deploy_root", default="output/deploy/"): FilePath(),
        Optional("source_stage", default="app_src.stage"): Regex(SCHEMA_AND_NAME),
        Optional("package"): RelaxedMap(
            {
                Optional("scripts", default=None): UniqueSeq(FilePath()),
                Optional("role"): Regex(IDENTIFIER),
                Optional("name"): Regex(IDENTIFIER),
                Optional("warehouse"): Regex(IDENTIFIER),
            }
        ),
        Optional("application"): RelaxedMap(
            {
                Optional("role"): Regex(IDENTIFIER),
                Optional("name"): Regex(IDENTIFIER),
                Optional("warehouse"): Regex(IDENTIFIER),
                Optional("debug", default=True): Bool(),
            }
        ),
    }
)
