from snowcli.plugins.connection import plugin_spec as connection_plugin_spec
from snowcli.plugins.containers import plugin_spec as containers_plugin_spec
from snowcli.plugins.nativeapp import plugin_spec as nativeapp_plugin_spec
from snowcli.plugins.object import plugin_spec as object_plugin_spec
from snowcli.plugins.registry import plugin_spec as registry_plugins_spec
from snowcli.plugins.render import plugin_spec as render_plugin_spec
from snowcli.plugins.snowpark import plugin_spec as snowpark_plugin_spec
from snowcli.plugins.sql import plugin_spec as sql_plugin_spec
from snowcli.plugins.streamlit import plugin_spec as streamlit_plugin_spec

# plugin name to plugin spec
builtin_plugin_name_to_plugin_spec = {
    "connection": connection_plugin_spec,
    "containers": containers_plugin_spec,
    "nativeapp": nativeapp_plugin_spec,
    "object": object_plugin_spec,
    "registry": registry_plugins_spec,
    "render": render_plugin_spec,
    "snowpark": snowpark_plugin_spec,
    "sql": sql_plugin_spec,
    "streamlit": streamlit_plugin_spec,
}
