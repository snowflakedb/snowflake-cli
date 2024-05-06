from abc import ABC, abstractmethod
from pathlib import Path

from click import ClickException
from snowflake.cli.api.project.schemas.native_app.native_app import NativeApp
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping


class MissingProjectDefinitionPropertyError(ClickException):
    """Missing value detected for a required Project Definition field."""

    def __init__(self, err_msg: str):
        super().__init__(err_msg)


class ArtifactProcessor(ABC):
    @abstractmethod
    def __init__(
        self,
        project_definition: NativeApp,
        project_root: Path,
        deploy_root: Path,
        artifact_to_process: PathMapping,
        **kwargs,
    ) -> None:
        pass

    @abstractmethod
    def process(self) -> None:
        pass
