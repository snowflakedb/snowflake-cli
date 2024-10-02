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

from snowflake.cli._plugins.connection import plugin_spec as connection_plugin_spec
from snowflake.cli._plugins.cortex import plugin_spec as cortex_plugin_spec
from snowflake.cli._plugins.git import plugin_spec as git_plugin_spec
from snowflake.cli._plugins.init import plugin_spec as init_plugin_spec
from snowflake.cli._plugins.nativeapp import plugin_spec as nativeapp_plugin_spec
from snowflake.cli._plugins.notebook import plugin_spec as notebook_plugin_spec
from snowflake.cli._plugins.object import plugin_spec as object_plugin_spec
from snowflake.cli._plugins.snowpark import plugin_spec as snowpark_plugin_spec
from snowflake.cli._plugins.spcs import plugin_spec as spcs_plugin_spec
from snowflake.cli._plugins.sql import plugin_spec as sql_plugin_spec
from snowflake.cli._plugins.stage import plugin_spec as stage_plugin_spec
from snowflake.cli._plugins.streamlit import plugin_spec as streamlit_plugin_spec
from snowflake.cli._plugins.workspace import plugin_spec as workspace_plugin_spec


# plugin name to plugin spec
def get_builtin_plugin_name_to_plugin_spec():
    plugin_specs = {
        "connection": connection_plugin_spec,
        "spcs": spcs_plugin_spec,
        "nativeapp": nativeapp_plugin_spec,
        "object": object_plugin_spec,
        "snowpark": snowpark_plugin_spec,
        "stage": stage_plugin_spec,
        "sql": sql_plugin_spec,
        "streamlit": streamlit_plugin_spec,
        "git": git_plugin_spec,
        "notebook": notebook_plugin_spec,
        "cortex": cortex_plugin_spec,
        "init": init_plugin_spec,
        "workspace": workspace_plugin_spec,
    }

    return plugin_specs
