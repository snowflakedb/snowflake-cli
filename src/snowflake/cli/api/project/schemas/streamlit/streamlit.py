from __future__ import annotations

from typing import List, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class Streamlit(UpdatableModel):
    name: str = Field(title="App identifier")
    stage: Optional[str] = Field(
        title="Stage in which the app’s artifacts will be stored", default="streamlit"
    )
    query_warehouse: str = Field(
        title="Snowflake warehouse to host the app", default="streamlit"
    )
    main_file: Optional[str] = Field(
        title="Entrypoint file of the Streamlit app", default="streamlit_app.py"
    )
    env_file: Optional[str] = Field(
        title="File defining additional configurations for the app, such as external dependencies",
        default=None,
    )
    pages_dir: Optional[str] = Field(title="Streamlit pages", default=None)
    additional_source_files: Optional[List[str]] = Field(
        title="List of additional files which should be included into deployment artifacts",
        default=None,
    )
