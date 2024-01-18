from __future__ import annotations

from snowflake.cli.api.project.schemas import (
    native_app,
    snowpark,
    streamlit,
)
from snowflake.cli.api.project.schemas.relaxed_map import RelaxedMap
from strictyaml import (
    Int,
    Optional,
)

project_schema = RelaxedMap(
    {
        "definition_version": Int(),
        Optional("native_app"): native_app.native_app_schema,
        Optional("snowpark"): snowpark.snowpark_schema,
        Optional("streamlit"): streamlit.streamlit_schema,
    }
)

project_override_schema = project_schema.as_fully_optional()
