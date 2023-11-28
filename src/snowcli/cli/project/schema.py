from __future__ import annotations

from strictyaml import (
    Any,
    Bool,
    Decimal,
    EmptyList,
    Int,
    MapCombined,
    MapPattern,
    Optional,
    Regex,
    Seq,
    Str,
    UniqueSeq,
)

from .util import IDENTIFIER, SCHEMA_AND_NAME

# TODO: use the util regexes to validate paths + globs
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

Argument = RelaxedMap({"name": Str(), "type": Str()})

_callable_mapping = {
    "name": Regex(IDENTIFIER),
    "handler": Str(),
    "returns": Str(),
    "signature": Seq(Argument) | EmptyList(),
    Optional("external_access_integration"): Seq(Str()),
    Optional("secrets"): MapPattern(Str(), Str()),
}

function_schema = RelaxedMap(_callable_mapping)

procedure_schema = RelaxedMap(
    {
        **_callable_mapping,
        Optional("execute_as_caller"): Bool(),
    }
)

streamlit_schema = RelaxedMap(
    {
        "name": Str(),
        "stage": Str(),
        "query_warehouse": Str(),
        Optional("main_file", default="streamlit_app.py"): FilePath(),
        Optional("env_file"): FilePath(),
        Optional("pages_dir"): FilePath(),
        Optional("additional_source_files"): Seq(FilePath()),
    }
)

snowpark_schema = RelaxedMap(
    {
        "project_name": Str(),
        "stage_name": Str(),
        "src": Str(),
        Optional("functions"): Seq(function_schema),
        Optional("procedures"): Seq(procedure_schema),
    }
)

project_schema = RelaxedMap(
    {
        "definition_version": Int(),
        Optional("native_app"): native_app_schema,
        Optional("snowpark"): snowpark_schema,
        Optional("streamlit"): streamlit_schema,
    }
)

project_override_schema = project_schema.as_fully_optional()
