from __future__ import annotations

import logging
import typer
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

    def create(
        self,
        streamlit_name: str,
        file: Path,
        from_stage: str,
        use_packaging_workaround: bool,
    ) -> SnowflakeCursor:
        connection = self._conn
        if from_stage:
            standard_page_name = StageManager.get_standard_stage_name(from_stage)
            from_stage_command = f"FROM {standard_page_name}"
        else:
            from_stage_command = ""
        main_file = (
            "streamlit_app_launcher.py" if use_packaging_workaround else file.name
        )

        return self._execute_query(
            f"""
            create streamlit {streamlit_name}
            {from_stage_command}
            MAIN_FILE = '{main_file}'
            QUERY_WAREHOUSE = {connection.warehouse};

            alter streamlit {streamlit_name} checkout;
        """
        )

    def share(self, streamlit_name: str, to_role: str) -> SnowflakeCursor:
        return self._execute_query(
            f"grant usage on streamlit {streamlit_name} to role {to_role}"
        )

    def drop(self, streamlit_name: str) -> SnowflakeCursor:
        return self._execute_query(f"drop streamlit {streamlit_name}")

    def deploy(
        self,
        streamlit_name: str,
        file: Path,
        open_in_browser: bool,
        use_packaging_workaround: bool,
        packaging_workaround_includes_content: bool,
        pypi_download: str,
        check_anaconda_for_pypi_deps: bool,
        package_native_libraries: str,
        excluded_anaconda_deps: str,
    ):
        stage_manager = StageManager()

        # THIS WORKAROUND HAS NOT BEEN TESTED WITH THE NEW STREAMLIT SYNTAX
        if use_packaging_workaround:
            self._packaging_workaround(
                streamlit_name,
                file,
                packaging_workaround_includes_content,
                pypi_download,
                check_anaconda_for_pypi_deps,
                package_native_libraries,
                excluded_anaconda_deps,
                stage_manager,
            )

        qualified_name = self.qualified_name(streamlit_name)
        streamlit_stage_name = f"snow://streamlit/{qualified_name}/default_checkout"
        stage_manager.put(str(file), streamlit_stage_name, 4, True)
        query_result = self._execute_query(
            f"call SYSTEM$GENERATE_STREAMLIT_URL_FROM_NAME('{streamlit_name}')"
        )
        base_url = query_result.fetchone()[0]
        url = self._get_url(base_url, qualified_name)

        if open_in_browser:
            typer.launch(url)
        else:
            return url

    def _packaging_workaround(
        self,
        streamlit_name: str,
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
        stage_name = f"{streamlit_name}_stage"
        stage_manager.put("app.zip", stage_name, 4, True)
        main_module = str(file).replace(".py", "")
        file = generate_streamlit_package_wrapper(
            stage_name=f"{streamlit_name}_stage",
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

    def _get_url(self, base_url: str, qualified_name: str) -> str:
        connection = self._conn

        if not connection.host:
            return base_url

        host_parts = connection.host.split(".")

        if len(host_parts) == 3:
            return base_url

        if len(host_parts) != 6:
            log.error(
                f"The connection host ({connection.host}) was missing or not in "
                "the expected format "
                "(<account>.<deployment>.snowflakecomputing.com)"
            )
            raise typer.Exit()
        else:
            account_name = host_parts[0]
            deployment = ".".join(host_parts[1:4])

        snowflake_host = connection.host or "app.snowflake.com"
        return (
            f"https://{snowflake_host}/{deployment}/{account_name}/"
            f"#/streamlit-apps/{qualified_name.upper()}"
        )

    def qualified_name(self, object_name: str):
        return f"{self._conn.database}.{self._conn.schema}.{object_name}"
