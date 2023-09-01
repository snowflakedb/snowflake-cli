from __future__ import annotations
from typing import cast
from strictyaml import (
    MapCombined,
    Seq,
    Int,
    Optional,
    UniqueSeq,
    Bool,
    Str,
    Any,
    Decimal,
    Regex,
    YAML,
)

IDENTIFIER = r'(?:("[^"]*(""[^"]*)*")|([A-Za-z_][\w$]{0,254}))'
SCHEMA_AND_NAME = f"{IDENTIFIER}[.]{IDENTIFIER}"
GLOB_REGEX = r"^[a-zA-Z0-9_\-./*?**\p{L}\p{N}]+$"
RELATIVE_PATH = r"^[^/][\p{L}\p{N}_\-.][^/]*$"

# TODO: use the above regexes to validate paths + globs
FilePath = Str
Glob = Str


class RelaxedMap(MapCombined):
    """
    A version of a Map that allows any number of unknown key/value pairs.
    """

    def __init__(self, map_validator):
        super().__init__(
            map_validator,
            Str(),
            # moves through value validators left-to-right until one matches
            Bool() | Decimal() | Int() | Any(),
        )

    def as_fully_optional(self) -> RelaxedMap:
        """
        Returns a copy of this schema with all its keys optional, recursing into other
        RelaxedMaps we find inside the schema. For existing optional keys, we strip out
        the default value and ensure we don't create any new keys.
        """
        validator = {}
        for key, value in self._validator_dict.items():
            validator[Optional(key)] = (
                value
                if not isinstance(value, RelaxedMap)
                else value.as_fully_optional()
            )
        return RelaxedMap(validator)


PathMapping = RelaxedMap(
    {
        "src": Glob() | Seq(Glob()),
        Optional("dest"): FilePath(),
    }
)

DEFAULT_PACKAGE_SCRIPTS = "package/*.sql"
native_app_schema = RelaxedMap(
    {
        "name": Str(),
        "artifacts": Seq(FilePath() | PathMapping),
        Optional("deploy_root", default="output/deploy/"): FilePath(),
        Optional("source_stage", default="app_src.stage"): Regex(SCHEMA_AND_NAME),
        Optional("package", default=dict(scripts=DEFAULT_PACKAGE_SCRIPTS)): RelaxedMap(
            {
                Optional("scripts", default=DEFAULT_PACKAGE_SCRIPTS): Glob()
                | UniqueSeq(Glob()),
                Optional("role"): Regex(IDENTIFIER),
                Optional("name"): Regex(IDENTIFIER),
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

project_schema = RelaxedMap(
    {
        "definition_version": Int(),
        "native_app": native_app_schema,
    }
)

project_override_schema = project_schema.as_fully_optional()
