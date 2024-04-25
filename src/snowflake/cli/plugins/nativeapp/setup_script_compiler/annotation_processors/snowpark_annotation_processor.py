from pathlib import Path
from textwrap import dedent
from typing import Dict, List, Tuple

# from snowflake.cli.plugins.nativeapp.utils import get_all_file_paths_under_dir
from click import UsageError
from snowflake.cli.plugins.nativeapp.artifacts import ArtifactMapping
from snowflake.cli.plugins.nativeapp.setup_script_compiler.annotation_processors.annotation_processor import (
    AnnotationProcessor,
)
from snowflake.cli.plugins.nativeapp.setup_script_compiler.snowpark_extension_function import (
    ExtensionFunctionProperties,
    convert_snowpark_object_to_internal_rep,
)


def verify_correct_version_of_snowpark_library() -> bool:
    """
    Verify that snowflake-snowpark-python has the right version, and the required parameters to set.
    """
    import snowflake.snowpark

    return (
        (snowflake.snowpark.__version__ >= "1.15.0")
        and hasattr(
            snowflake.snowpark.context, "_is_execution_environment_sandboxed_for_client"
        )
        and hasattr(snowflake.snowpark.context, "_should_continue_registration")
    )


class SnowparkAnnotationProcessor(AnnotationProcessor):
    def __init__(self):
        super().__init__()
        if not verify_correct_version_of_snowpark_library():
            raise UsageError(
                dedent(
                    f"""\
                    The installed snowflake-snowpark-python library is on an older release version, cannot use Python annotation processor with this version.
                    Please upgrade the library to >= 1.15.0 or remove the use of processor from artifacts list in your project definition file(s).
                """
                )
            )

    def get_py_file_to_artifact_map(
        self, artifacts: List[ArtifactMapping], project_root: Path, deploy_root: Path
    ) -> Dict[Path, Tuple[Path, Path]]:
        artifacts_to_process: List[ArtifactMapping] = [
            # TODO: change artifact.processor condition once non-Snowpark annotation processors are available
            artifact
            for artifact in artifacts
            if artifact.processor is not None
        ]

        py_file_to_dest_map: Dict[Path, str] = {}
        py_file_to_artifact_map: Dict[Path, Tuple[Path, Path]] = {}

        for artifact in artifacts_to_process:
            raw_src_path = Path(project_root, artifact.src)
            raw_dest_path = Path(deploy_root, artifact.dest)

            if raw_src_path.suffix == ".py" and raw_dest_path.suffix == ".py":
                py_file_to_artifact_map[raw_src_path] = (raw_src_path, raw_dest_path)

        return py_file_to_artifact_map

        # TODO: follow up on symlinking discussion
        # for artifact in artifacts_to_process:
        #     # Get all paths, directory or otherwise, that fall under the artifact.src
        #     paths_in_artifact_source = set(
        #         get_source_paths(artifact=artifact, project_root=project_root)
        #     )
        #     # For snowpark processor, this will hold all python files to process
        #     python_files_in_artifact_source = set()

        #     for path in paths_in_artifact_source:
        #         if path.is_dir():
        #             # if path is a directory, then either the user specified * as a glob, in which case we symlink the entire directory and its contents
        #             # or the user specified **/* or **/*.py etc as a glob, in which case it will already be included in all_paths_under_artifact_source.
        #             paths_in_artifact_source.update(
        #                 get_all_file_paths_under_dir(path)
        #             )
        #         elif path.suffix == ".py":
        #             python_files_in_artifact_source.add(path)
        #             py_file_to_dest_map[path] = artifact.dest

        # return py_file_to_dest_map

    def parse_annotation_data_in_files(
        self, py_file_to_artifact_map: Dict[Path, Tuple[Path, Path]]
    ) -> Dict[Path, List[ExtensionFunctionProperties]]:
        py_file_to_cli_snowpark_properties_map: Dict[
            Path, List[ExtensionFunctionProperties]
        ] = {}
        for py_file in py_file_to_artifact_map.keys():
            raw_snowpark_extension_function_properties_lst: List = []
            self.invoke_processor_on_py_file(
                py_file, raw_snowpark_extension_function_properties_lst
            )  # The callback to fetch raw properties
            cli_extension_function_properties = map(
                convert_snowpark_object_to_internal_rep,
                raw_snowpark_extension_function_properties_lst,
            )
            py_file_to_cli_snowpark_properties_map[py_file] = list(
                cli_extension_function_properties
            )
        return py_file_to_cli_snowpark_properties_map

    def invoke_processor_on_py_file(
        self, py_file: Path, extension_function_properties_lst: List
    ):
        # This is the callback that will get required data from the Snowpark library
        def callback_replacement(callable_properties) -> bool:
            extension_function_properties_lst.append(callable_properties)
            return False  # To not continue registration in Snowpark

        import snowflake.snowpark.context as ctx
        import snowflake.snowpark.session as session

        ctx._is_execution_environment_sandboxed_for_client = True  # noqa: SLF001
        ctx._should_continue_registration = callback_replacement  # noqa: SLF001
        session._is_execution_environment_sandboxed_for_client = True  # noqa: SLF001

        with open(py_file, mode="r", encoding="utf-8") as udf_code:
            code = udf_code.read()

        exec(code, globals())

    def set_properties_on_cli_snowpark_objects(
        self,
        py_file_to_cli_snowpark_properties_map: Dict[
            Path, List[ExtensionFunctionProperties]
        ],
        py_file_to_artifact_map: Dict[Path, Tuple[Path, Path]],
        deploy_root: Path,
    ) -> None:
        for py_file in py_file_to_cli_snowpark_properties_map:
            cli_snowpark_properties_lst = py_file_to_cli_snowpark_properties_map[
                py_file
            ]
            for snowpark_obj in cli_snowpark_properties_lst:
                snowpark_obj.set_source_file(py_file)
                snowpark_obj.set_destination_file(py_file_to_artifact_map[py_file][1])
                snowpark_obj.set_handler()
                snowpark_obj.set_deploy_root(deploy_root)

    def get_annotation_data_from_files(
        self,
        artifacts: List[ArtifactMapping],
        project_root: Path,
        deploy_root: Path,
    ) -> Tuple[
        Dict[Path, Tuple[Path, Path]], Dict[Path, List[ExtensionFunctionProperties]]
    ]:
        py_file_to_artifact_map = self.get_py_file_to_artifact_map(
            artifacts=artifacts, project_root=project_root, deploy_root=deploy_root
        )

        py_file_to_cli_snowpark_properties_map = self.parse_annotation_data_in_files(
            py_file_to_artifact_map=py_file_to_artifact_map
        )

        self.set_properties_on_cli_snowpark_objects(
            py_file_to_cli_snowpark_properties_map=py_file_to_cli_snowpark_properties_map,
            py_file_to_artifact_map=py_file_to_artifact_map,
            deploy_root=deploy_root,
        )

        return py_file_to_artifact_map, py_file_to_cli_snowpark_properties_map
