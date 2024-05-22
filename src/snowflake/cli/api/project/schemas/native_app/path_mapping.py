from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import Field, field_validator
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel


class ProcessorMapping(UpdatableModel):
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
    processors: Optional[List[Union[str, ProcessorMapping]]] = []

    @field_validator("processors")
    @classmethod
    def transform_processors(
        cls, input_values: Optional[List[Union[str, Dict, ProcessorMapping]]]
    ):
        if input_values is None:
            return []

        transformed_processors: List[ProcessorMapping] = []
        for input_processor in input_values:
            if isinstance(input_processor, str):
                transformed_processors.append(ProcessorMapping(name=input_processor))
            elif isinstance(input_processor, Dict):
                transformed_processors.append(ProcessorMapping(**input_processor))
            else:
                transformed_processors.append(input_processor)
        return transformed_processors
