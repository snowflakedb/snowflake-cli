from __future__ import annotations

from snowcli.cli.project.schemas.relaxed_map import FilePath, RelaxedMap
from strictyaml import (
    Optional,
    Seq,
    Str,
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
