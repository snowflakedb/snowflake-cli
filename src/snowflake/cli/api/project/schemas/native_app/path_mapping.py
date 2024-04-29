from __future__ import annotations

from typing import List, Literal, Optional, Union

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel

# Limited to python options only for now.
# Also, processor name can only be "snowpark" for now.
VirtualEnvTypeOptions = Literal["conda", "venv"]


class VirtualEnvironment(UpdatableModel):
    name: str = Field(
        title="Name of the virtual environment to run the processor in.",
    )
    env_type: VirtualEnvTypeOptions = Field(
        title="Type of the virtual environment to activate.",
    )


class AnnotationProcessor(UpdatableModel):
    name: str = Field(
        title="Name of the processor to be invoked to discover the annotated code."
    )
    virtual_env: Optional[VirtualEnvironment] = Field(
        title="The virtual environment to run the processor in. If none is provided, then the Snowflake CLI will try to get the current environment information.",
        default=None,
    )


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
    processors: Optional[List[Union[str, AnnotationProcessor]]] = None
