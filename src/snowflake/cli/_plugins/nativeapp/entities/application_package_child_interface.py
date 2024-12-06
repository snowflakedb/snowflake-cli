from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class ApplicationPackageChildInterface(ABC):
    @abstractmethod
    def bundle(self, bundle_root=Path, *args, **kwargs) -> None:
        """
        Bundles the entity artifacts into the provided root directory. Must not have any side-effects, such as deploying the artifacts into a stage, etc.
        @param bundle_root: The directory where the bundle contents should be put.
        """
        pass

    @abstractmethod
    def get_deploy_sql(
        self,
        artifacts_dir: Path,
        schema: Optional[str],
        *args,
        **kwargs,
    ) -> str:
        """
        Returns the SQL that would create the entity object. Must not execute the SQL or have any other side-effects.
        @param artifacts_dir: Path to the child entity artifacts directory relative to the deploy root.
        @param [Optional] schema: Schema to use when creating the object.
        """
        pass

    @abstractmethod
    def get_usage_grant_sql(
        self,
        app_role: str,
        schema: Optional[str],
        *args,
        **kwargs,
    ) -> str:
        """
        Returns the SQL that would grant the required USAGE privilege to the provided application role on the entity object. Must not execute the SQL or have any other side-effects.
        @param app_role: The application role to grant the privileges to.
        @param [Optional] schema: The schema where the object was created.
        """
        pass
