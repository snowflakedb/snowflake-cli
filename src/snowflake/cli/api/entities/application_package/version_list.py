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

from snowflake.cli._plugins.workspace.manager import ActionContext
from snowflake.cli.api.entities.actions import EntityActions
from snowflake.cli.api.entities.application_package.entity import (
    ApplicationPackageEntity,
)

ApplicationPackageEntity.implements(EntityActions.VERSION_LIST)


def action_version_list(self, ctx: ActionContext):
    """Lists all versions defined in an application package."""
    pass
