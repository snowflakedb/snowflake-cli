from abc import ABC, abstractmethod
from pathlib import Path

from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping


class BuiltInAnnotationProcessor(ABC):
    @abstractmethod
    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
        artifact_to_process: PathMapping,
    ) -> None:
        pass

    @abstractmethod
    def process(self) -> None:
        pass
