from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import (
    PathMapping,
    ProcessorMapping,
)


class ArtifactProcessor(ABC):
    @abstractmethod
    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
        **kwargs,
    ) -> None:
        pass

    @abstractmethod
    def process(
        self,
        artifact_to_process: PathMapping,
        processor_mapping: Optional[ProcessorMapping],
        **kwargs,
    ) -> None:
        pass
