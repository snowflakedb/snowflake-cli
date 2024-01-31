from snowflake.cli.plugins.connection import plugin_spec as connection_plugin_spec
from snowflake.cli.plugins.nativeapp import plugin_spec as nativeapp_plugin_spec
from snowflake.cli.plugins.object import plugin_spec as object_plugin_spec
from snowflake.cli.plugins.render import plugin_spec as render_plugin_spec
from snowflake.cli.plugins.snowpark import plugin_spec as snowpark_plugin_spec
from snowflake.cli.plugins.spcs import plugin_spec as spcs_plugin_spec
from snowflake.cli.plugins.sql import plugin_spec as sql_plugin_spec
from snowflake.cli.plugins.streamlit import plugin_spec as streamlit_plugin_spec

# plugin name to plugin spec
builtin_plugin_name_to_plugin_spec = {
    "connection": connection_plugin_spec,
    "spcs": spcs_plugin_spec,
    "nativeapp": nativeapp_plugin_spec,
    "object": object_plugin_spec,
    "render": render_plugin_spec,
    "snowpark": snowpark_plugin_spec,
    "sql": sql_plugin_spec,
    "streamlit": streamlit_plugin_spec,
}
