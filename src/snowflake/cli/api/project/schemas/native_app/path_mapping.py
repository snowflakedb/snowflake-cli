from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class AnnotationProcessor(UpdatableModel):
    name: str = Field(
        title="Name of the processor to be invoked to discover the annotated code."
    )
    properties: Optional[Dict[str, Any]] = Field(
        title="Properties to aid the invocation of the processor on the annotated code. If none are provided, then the Snowflake CLI will try to get the current environment information.",
        default=None,
    )


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
    processors: Optional[List[Union[str, AnnotationProcessor]]] = None
