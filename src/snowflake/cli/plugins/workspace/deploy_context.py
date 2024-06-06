from __future__ import annotations

from typing import Dict

from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.plugins.workspace.entities.application_entity import (
    ApplicationEntity,
)
from snowflake.cli.plugins.workspace.entities.application_package_entity import (
    ApplicationPackageEntity,
)
from snowflake.cli.plugins.workspace.entities.streamlit_entity import StreamlitEntity
from snowflake.cli.plugins.workspace.entities.table_entity import TableEntity

entity_map = {
    "application": ApplicationEntity,
    "application package": ApplicationPackageEntity,
    "streamlit": StreamlitEntity,
    "table": TableEntity,
}


class DeployContext:
    def __init__(self, workspace_definition: Dict):
        self.workspace_definition = workspace_definition
        self.entities: Dict = {}

    def register_entity(self, entity_key, entity_config):
        entity_config["key"] = entity_key
        if "extends" in entity_config:
            source = self.get_entity(entity_config["extends"])
            entity_config = {**source.config, **entity_config}
        entity_type = entity_config["type"]
        if entity_type not in entity_map:
            raise ValueError(f"Unsupported entity type: {entity_type}")
        self.entities[entity_key] = entity_map[entity_type](entity_config)

    def get_entity(self, key):
        if key not in self.entities:
            raise ValueError(f"No such entity: {key}")
        return self.entities[key]

    def get_wh_name(self):
        return cli_context.connection.warehouse

    def get_db_name(self):
        return cli_context.connection.database

    def get_schema_name(self):
        return self.workspace_definition["stage"]["schema"]

    def get_stage_name(self):
        return self.workspace_definition["stage"]["name"]
