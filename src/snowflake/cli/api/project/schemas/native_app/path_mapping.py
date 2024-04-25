from __future__ import annotations

from typing import List, Literal, Optional

from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel

ProcessorOptions = Literal["python-snowpark", "PYTHON-SNOWPARK"]


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
    processor: Optional[List[ProcessorOptions]] = None
