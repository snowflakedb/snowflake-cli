from abc import ABC, abstractmethod


class ApplicationPackageChildInterface(ABC):
    @abstractmethod
    def bundle(self, bundle_root=None, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get_deploy_sql(
        self,
        *args,
        **kwargs,
    ) -> str:
        pass
