from pathlib import Path

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    Processor,
)
from snowflake.cli.plugins.nativeapp.codegen.constants import SNOWPARK_PROCESSOR
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)


def find_and_execute_processors(
    project_definition: NativeApp, project_root: Path, deploy_root: Path
):
    """
    Go through every artifact object in the project definition of a native app, and execute processors in order of specification for each of the artifact object.
    """
    artifacts = [
        artifact
        for artifact in project_definition.artifacts
        if isinstance(artifact, PathMapping)
    ]

    for artifact in artifacts:
        for processor in artifact.processors:
            if (isinstance(processor, str) and processor == SNOWPARK_PROCESSOR) or (
                isinstance(processor, Processor)
                and processor.name == SNOWPARK_PROCESSOR
            ):
                SnowparkAnnotationProcessor(
                    project_definition=project_definition,
                    project_root=project_root,
                    deploy_root=deploy_root,
                    artifact_to_process=artifact,
                    processor=processor,
                ).process()
