from __future__ import annotations

from snowflake.cli.api.project.schemas.relaxed_map import FilePath, RelaxedMap
from snowflake.cli.api.project.util import IDENTIFIER
from strictyaml import (
    Optional,
    Regex,
    Seq,
    Str,
)

streamlit_schema = RelaxedMap(
    {
        "name": Str(),
        Optional("database", default=None): Regex(IDENTIFIER),
        Optional("schema", default=None): Regex(IDENTIFIER),
        Optional("stage", default="streamlit"): Str(),
        "query_warehouse": Str(),
        Optional("main_file", default="streamlit_app.py"): FilePath(),
        Optional("env_file"): FilePath(),
        Optional("pages_dir"): FilePath(),
        Optional("additional_source_files"): Seq(FilePath()),
    }
)
