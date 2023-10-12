from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from snowflake.connector.cursor import SnowflakeCursor

from snowcli.cli.common.sql_execution import SqlExecutionMixin
from snowcli.cli.snowpark_shared import snowpark_package
from snowcli.cli.stage.manager import StageManager
from snowcli.utils import (
    generate_streamlit_environment_file,
    generate_streamlit_package_wrapper,
)
from snowcli.cli.connection.util import make_snowsight_url, MissingConnectionHostError
from snowcli.cli.project.util import unquote_identifier

log = logging.getLogger(__name__)


class StreamlitManager(SqlExecutionMixin):
    def list(self) -> SnowflakeCursor:
        return self._execute_query("show streamlits")

    def describe(self, streamlit_name: str) -> Tuple[SnowflakeCursor, SnowflakeCursor]:
        description = self._execute_query(f"describe streamlit {streamlit_name}")
        url = self._execute_query(
            f"call SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{streamlit_name}')"
        )
        return description, url

    def share(self, streamlit_name: str, to_role: str) -> SnowflakeCursor:
        return self._execute_query(
            f"grant usage on streamlit {streamlit_name} to role {to_role}"
        )

    def drop(self, streamlit_name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop streamlit {streamlit_name}")

    def deploy(
        self,
        streamlit_name: str,
        main_file: Path,
        environment_file: Optional[Path] = None,
        pages_dir: Optional[Path] = None,
        stage_name: Optional[str] = None,
        warehouse: Optional[str] = None,
        replace: Optional[bool] = False,
    ):
        stage_manager = StageManager()

        stage_name = stage_name or "streamlit"
        stage_name = stage_manager.to_fully_qualified_name(stage_name)

        stage_manager.create(stage_name=stage_name)

        root_location = stage_manager.get_standard_stage_name(
            f"{stage_name}/{streamlit_name}"
        )

        stage_manager.put(main_file, root_location, 4, True)

        if environment_file and environment_file.exists():
            stage_manager.put(environment_file, root_location, 4, True)

        if pages_dir and pages_dir.exists():
            stage_manager.put(pages_dir / "*", f"{root_location}/pages", 4, True)

        replace_stmt = "OR REPLACE" if replace else ""
        use_warehouse_stmt = f"QUERY_WAREHOUSE = {warehouse}" if warehouse else ""
        self._execute_query(
            f"""
            CREATE {replace_stmt} STREAMLIT {streamlit_name}
            ROOT_LOCATION = '{root_location}'
            MAIN_FILE = '{main_file.name}'
            {use_warehouse_stmt}
        """
        )

        return self.get_url(streamlit_name)

    def _packaging_workaround(
        self,
        streamlit_name: str,
        stage_name: str,
        file: Path,
        packaging_workaround_includes_content: bool,
        pypi_download: str,
        check_anaconda_for_pypi_deps: bool,
        package_native_libraries: str,
        excluded_anaconda_deps: str,
        stage_manager: StageManager,
    ):
        # package an app.zip file, same as the other snowpark_containers_cmds package commands
        snowpark_package(
            pypi_download,  # type: ignore[arg-type]
            check_anaconda_for_pypi_deps,
            package_native_libraries,  # type: ignore[arg-type]
        )

        # upload the resulting app.zip file
        stage_name = stage_name or f"{streamlit_name}_stage"
        stage_manager.put("app.zip", stage_name, 4, True)
        main_module = str(file).replace(".py", "")
        file = generate_streamlit_package_wrapper(
            stage_name=stage_name,
            main_module=main_module,
            extract_zip=packaging_workaround_includes_content,
        )

        # upload the wrapper file
        stage_manager.put(str(file), stage_name, 4, True)

        # if the packaging process generated an environment.snowflake.txt
        # file, convert it into an environment.yml file
        excluded_anaconda_deps_list: Optional[List[str]] = None
        if excluded_anaconda_deps is not None:
            excluded_anaconda_deps_list = excluded_anaconda_deps.split(",")
        env_file = generate_streamlit_environment_file(excluded_anaconda_deps_list)
        if env_file:
            stage_manager.put(str(env_file), stage_name, 4, True)

    def get_url(self, streamlit_name: str) -> str:
        try:
            return make_snowsight_url(
                self._conn,
                f"/#/streamlit-apps/{self.qualified_name_for_url(streamlit_name)}",
            )
        except MissingConnectionHostError as e:
            return "https://app.snowflake.com"

    def qualified_name(self, object_name: str):
        return f"{self._conn.database}.{self._conn.schema}.{object_name}"

    def qualified_name_for_url(self, object_name: str):
        return (
            f"{unquote_identifier(self._conn.database)}."
            f"{unquote_identifier(self._conn.schema)}."
            f"{unquote_identifier(object_name)}"
        )
