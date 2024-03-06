from __future__ import annotations

from typing import Optional

from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
