from pathlib import Path
from typing import Optional, Union

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    Processor,
)
from snowflake.cli.plugins.nativeapp.codegen.artifact_processor import (
    ArtifactProcessor,
    MissingProjectDefinitionPropertyError,
)
from snowflake.cli.plugins.nativeapp.codegen.snowpark.python_processor import (
    SnowparkAnnotationProcessor,
)

SNOWPARK_PROCESSOR = "snowpark"


def _try_create_processor(
    processor: Union[str, Processor],
    project_definition: NativeApp,
    project_root: Path,
    deploy_root: Path,
    artifact_to_process: PathMapping,
    **kwargs,
) -> Optional[ArtifactProcessor]:
    if (isinstance(processor, str) and processor.lower() == SNOWPARK_PROCESSOR) or (
        isinstance(processor, Processor)
        and processor.name.lower() == SNOWPARK_PROCESSOR
    ):
        return SnowparkAnnotationProcessor(
            project_definition=project_definition,
            project_root=project_root,
            deploy_root=deploy_root,
            artifact_to_process=artifact_to_process,
            processor=processor,
        )
    else:
        return None


def _find_and_execute_processors(
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
            artifact_processor = _try_create_processor(
                project_definition=project_definition,
                project_root=project_root,
                deploy_root=deploy_root,
                artifact_to_process=artifact,
                processor=processor,
            )
            if artifact_processor is None:
                raise MissingProjectDefinitionPropertyError(
                    f"{processor if isinstance(processor, str) else processor.name} is not a valid processor type for artifacts in the project definition file."
                )
            else:
                artifact_processor.process()
