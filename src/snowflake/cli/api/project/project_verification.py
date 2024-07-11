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

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.exceptions import NoProjectDefinitionError


def assert_project_type(project_type: str):
    if not getattr(cli_context.project_definition, project_type, None):
        raise NoProjectDefinitionError(
            project_type=project_type, project_file=cli_context.project_root
        )
