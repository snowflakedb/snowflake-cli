from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Tuple

from snowflake.cli.plugins.nativeapp.artifacts import ArtifactMapping
from snowflake.cli.plugins.nativeapp.setup_script_compiler.snowpark_extension_function import (
    ExtensionFunctionProperties,
)


class AnnotationProcessor(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def get_annotation_data_from_files(
        self, artifacts: List[ArtifactMapping], project_root: Path
    ) -> Tuple[
        Dict[Path, Tuple[Path, Path]], Dict[Path, List[ExtensionFunctionProperties]]
    ]:
        pass
