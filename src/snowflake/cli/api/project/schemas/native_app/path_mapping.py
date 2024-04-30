from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class AnnotationProcessor(UpdatableModel):
    name: str = Field(
        title="Name of a processor to invoke on a collection of artifacts."
    )
    properties: Optional[Dict[str, Any]] = Field(
        title="A set of key-value pairs used to configure the output of the processor. Consult a specific processor's documentation for more details on the supported properties.",
        default=None,
    )


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
    processors: Optional[List[Union[str, AnnotationProcessor]]] = None
