# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    src: str = Field(
        title="Source path or glob pattern (relative to project root)", default=None
    )

    dest: Optional[str] = Field(
        title="Destination path on stage",
        description="Paths are relative to stage root; paths ending with a slash indicate that the destination is a directory which source files should be copied into.",
        default=None,
    )

    processors: Optional[List[Union[str, ProcessorMapping]]] = Field(
        title="List of processors to apply to matching source files during bundling.",
        default=[],
    )

    @field_validator("processors")
    @classmethod
    def transform_processors(
        cls, input_values: Optional[List[Union[str, Dict, ProcessorMapping]]]
    ) -> List[ProcessorMapping]:
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
