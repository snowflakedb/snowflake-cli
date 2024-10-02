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

from typing import Dict, List, Union, get_args

from snowflake.cli.api.entities.application_entity import ApplicationEntity
from snowflake.cli.api.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.api.entities.snowpark_entity import FunctionEntity, ProcedureEntity
from snowflake.cli.api.entities.streamlit_entity import StreamlitEntity
from snowflake.cli.api.project.schemas.entities.application_entity_model import (
    ApplicationEntityModel,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity_model import (
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.project.schemas.entities.snowpark_entity import (
    FunctionEntityModel,
    ProcedureEntityModel,
)
from snowflake.cli.api.project.schemas.entities.streamlit_entity_model import (
    StreamlitEntityModel,
)

Entity = Union[
    ApplicationEntity,
    ApplicationPackageEntity,
    StreamlitEntity,
    ProcedureEntity,
    FunctionEntity,
]
EntityModel = Union[
    ApplicationEntityModel,
    ApplicationPackageEntityModel,
    StreamlitEntityModel,
    FunctionEntityModel,
    ProcedureEntityModel,
]

ALL_ENTITIES: List[Entity] = [*get_args(Entity)]
ALL_ENTITY_MODELS: List[EntityModel] = [*get_args(EntityModel)]

v2_entity_model_types_map = {e.get_type(): e for e in ALL_ENTITY_MODELS}
v2_entity_model_to_entity_map: Dict[EntityModel, Entity] = {
    e.get_entity_model_type(): e for e in ALL_ENTITIES
}
