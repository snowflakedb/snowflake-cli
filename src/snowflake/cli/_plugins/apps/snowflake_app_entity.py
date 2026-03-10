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

from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
    SnowflakeAppEntityModel,
)
from snowflake.cli.api.entities.common import EntityBase


class SnowflakeAppEntity(EntityBase[SnowflakeAppEntityModel]):
    """Entity class for Snowflake App (snowflake-app) type."""

    pass
