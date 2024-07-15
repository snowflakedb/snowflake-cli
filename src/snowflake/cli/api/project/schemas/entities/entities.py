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

from typing import Union, get_args

from snowflake.cli.api.project.schemas.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.api.project.schemas.entities.application_package_entity import (
    ApplicationPackageEntity,
)

Entity = Union[ApplicationEntity, ApplicationPackageEntity]

ALL_ENTITIES = [*get_args(Entity)]

v2_entity_types_map = {e.get_type(): e for e in ALL_ENTITIES}
