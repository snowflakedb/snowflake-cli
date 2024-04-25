from pathlib import Path
from typing import Dict, List, Tuple

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.plugins.nativeapp.artifacts import find_setup_script_file
from snowflake.cli.plugins.nativeapp.setup_script_compiler.annotation_processors.snowpark_annotation_processor import (
    SnowparkAnnotationProcessor,
)
from snowflake.cli.plugins.nativeapp.setup_script_compiler.snowpark_extension_function import (
    ExtensionFunctionProperties,
)


def create_new_sql_file_at_path(parent_dir: Path, file_name: str) -> Path:
    sql_file_name = f"__{file_name}__.sql"
    sql_file_path = parent_dir / sql_file_name
    sql_file_path.write_text("")
    return sql_file_path


class SetupScriptCompiler:
    def __init__(
        self, project_definition: NativeApp, project_root: Path, deploy_root: Path
    ):
        self.project_root = project_root
        self.project_definition = project_definition
        self.deploy_root = deploy_root

    def parse_entities(self):
        snowpark_processor = SnowparkAnnotationProcessor()
        # TODO: remove instance variable and pass directly to generate_sql
        (
            self.py_file_to_artifact_map,
            self.py_file_to_cli_snowpark_properties_map,
        ) = snowpark_processor.get_annotation_data_from_files(
            artifacts=self.project_definition.artifacts,
            project_root=self.project_root,
            deploy_root=self.deploy_root,
        )

    def validate_entities(self):
        pass

    def generate_sql(self):
        py_file_to_sql_file_map = self.write_sql_ddl_to_file(
            py_file_to_artifact_map=self.py_file_to_artifact_map,
            py_file_to_cli_snowpark_properties_map=self.py_file_to_cli_snowpark_properties_map,
        )

        self.edit_setup_script_with_exec_imm_sql(
            py_file_to_sql_file_map=py_file_to_sql_file_map
        )

    def write_sql_ddl_to_file(
        self,
        py_file_to_artifact_map: Dict[Path, Tuple[Path, Path]],
        py_file_to_cli_snowpark_properties_map: Dict[
            Path, List[ExtensionFunctionProperties]
        ],
    ) -> Dict[Path, Path]:
        py_file_to_sql_file_map: Dict[Path, Path] = {}
        for py_file in py_file_to_artifact_map:
            # Create a new SQL file at the same location as destination py file
            dest_file = py_file_to_artifact_map[py_file][1]
            sql_file_path = create_new_sql_file_at_path(
                parent_dir=dest_file.parent, file_name=py_file.name.split(".")[0]
            )
            py_file_to_sql_file_map[py_file] = sql_file_path

        # For every Snowpark object in the source file, generate its SQL and write to SQL file
        for py_file in py_file_to_sql_file_map:
            sql_file_path = py_file_to_sql_file_map[py_file]
            for extension_function in py_file_to_cli_snowpark_properties_map[py_file]:
                with open(str(sql_file_path), "a") as file:
                    file.write(extension_function.generate_create_sql_statement())
                    file.write("\n")
                    file.write(extension_function.generate_grant_sql_statements())
                    file.write("\n")

        return py_file_to_sql_file_map

    def edit_setup_script_with_exec_imm_sql(
        self,
        py_file_to_sql_file_map: Dict[Path, Path],
    ):
        # For every SQL file, add SQL statement 'execute immediate' to setup script.
        _, dest_file = find_setup_script_file(deploy_root=self.deploy_root)
        # for _, sql_file_path in py_file_to_sql_file_map:
        for py_file in py_file_to_sql_file_map:
            sql_file_path = py_file_to_sql_file_map[py_file]
            with open(str(dest_file), "a") as file:
                file.write("\n")
                file.write(f"EXECUTE IMMEDIATE FROM {str(sql_file_path)}")
