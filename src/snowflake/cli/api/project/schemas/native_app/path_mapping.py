from __future__ import annotations

from typing import List, Literal, Optional

from pydantic import Field
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel

# Limited to python options only for now.
ProcessorOptions = Literal["python-snowpark", "PYTHON-SNOWPARK"]
VirtualEnvTypeOptions = Literal["conda", "CONDA", "venv", "VENV"]
LanguageOptions = Literal["python", "PYTHON"]


class VirtualEnvironment(UpdatableModel):
    name: str = Field(
        title="Name of the virtual environment to run the processor in.",
    )
    env_type: VirtualEnvTypeOptions = Field(
        title="Type of the virtual environment to activate.",
        default="venv",
    )


class AnnotationProcessor(UpdatableModel):
    language: LanguageOptions = Field(
        title="Programming language in which the annotated code is written.",
    )
    language_version: str = Field(
        title="Programming language version in which the annotated code is written, and to use in a virtual environment, if provided.",
    )
    name: ProcessorOptions = Field(
        title="Name of the processor to be invoked to discover the annotated code."
    )
    virtual_env: Optional[VirtualEnvironment] = Field(
        title="The virtual environment to run the processor in.",
        default=None,
    )


class PathMapping(UpdatableModel):
    src: str
    dest: Optional[str] = None
    processors: Optional[List[AnnotationProcessor]] = None
