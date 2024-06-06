from __future__ import annotations

from pathlib import Path

from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.schemas.streamlit.streamlit import Streamlit
from snowflake.cli.plugins.workspace.entities.entity_base import Entity


class StreamlitEntity(Entity):
    def __init__(self, entity_config):
        super().__init__(entity_config)

    def create_deploy_plan_impl(self, ctx, plan, parent=None, *args, **kwargs):
        if not parent and "stage" in self.config:
            stage_name = self.config["stage"]
        else:
            stage_name = ctx.get_stage_name()

        db_name = ctx.get_db_name()
        schema_name = ctx.get_schema_name()
        streamlit_name = self.config["name"]

        # Existing code, deploys the streamlit: src/snowflake/cli/plugins/streamlit/manager.py
        # url = StreamlitManager().deploy(
        #     streamlit=streamlit_name,
        #     stage_name=stage_name,
        #     main_file=Path(streamlit.main_file),
        #     replace=True,
        # )

        streamlit = Streamlit(
            name=self.config["name"],
            main_file=self.config["main_file"],
        )
        streamlit_name = FQN.from_identifier_model(streamlit).using_context()
        plan.add_sql(
            f"CREATE STAGE IF NOT EXISTS {db_name}.{schema_name}.{stage_name};"
        )
        plan.add_sql(
            f"""PUT file://{Path(streamlit.main_file)} @{db_name}.{schema_name}.{stage_name}/{streamlit_name}
    auto_compress=false
    parallel=4
    overwrite=True;"""
        )
        plan.add_sql(
            f"""CREATE OR REPLACE STREAMLIT {db_name}.{schema_name}.{streamlit_name}
    ROOT_LOCATION = '@{db_name}.{schema_name}.{stage_name}/{streamlit_name}'
    MAIN_FILE = '{streamlit.main_file}';"""
        )
