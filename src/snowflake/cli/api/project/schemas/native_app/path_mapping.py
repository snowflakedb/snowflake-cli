from __future__ import annotations

from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class PathMapping(UpdatableModel):
    src: str
    dest: str | None = None
