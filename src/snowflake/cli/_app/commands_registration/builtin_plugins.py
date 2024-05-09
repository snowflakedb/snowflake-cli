from snowflake.cli._plugins.connection import plugin_spec as connection_plugin_spec
from snowflake.cli._plugins.git import plugin_spec as git_plugin_spec
from snowflake.cli._plugins.nativeapp import plugin_spec as nativeapp_plugin_spec
from snowflake.cli._plugins.notebook import plugin_spec as notebook_plugin_spec
from snowflake.cli._plugins.object import plugin_spec as object_plugin_spec
from snowflake.cli._plugins.snowpark import plugin_spec as snowpark_plugin_spec
from snowflake.cli._plugins.spcs import plugin_spec as spcs_plugin_spec
from snowflake.cli._plugins.sql import plugin_spec as sql_plugin_spec
from snowflake.cli._plugins.stage import plugin_spec as stage_plugin_spec
from snowflake.cli._plugins.streamlit import plugin_spec as streamlit_plugin_spec


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
    }

    return plugin_specs
