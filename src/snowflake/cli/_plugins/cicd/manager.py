import enum
from typing import List, Optional, Type

from snowflake.cli.api.secure_path import SecurePath


class CIProviderChoices(str, enum.Enum):
    GITHUB = "GITHUB"
    GITLAB = "GITLAB"


class CIProvider:
    NAME: str

    @classmethod
    def cleanup(cls, root: SecurePath) -> None:
        raise NotImplementedError()

    @classmethod
    def from_choice(cls, choice: CIProviderChoices) -> "CIProvider":
        return {
            GithubProvider.NAME: GithubProvider,
            GitLabProvider.NAME: GitLabProvider,
        }[choice.name]()

    @classmethod
    def all(cls) -> List[Type["CIProvider"]]:  # noqa: A003
        return [GithubProvider, GitLabProvider]

    def has_template(self, root_dir: SecurePath) -> bool:
        raise NotImplementedError()

    def copy(self, source: SecurePath, destination: SecurePath):
        raise NotImplementedError()


class GithubProvider(CIProvider):
    NAME = CIProviderChoices.GITHUB.name

    @classmethod
    def cleanup(cls, root_dir: SecurePath):
        (root_dir / ".github").rmdir(recursive=True)

    def has_template(self, root_dir: SecurePath) -> bool:
        return (root_dir / ".github/workflows").exists()

    def copy(self, source: SecurePath, destination: SecurePath) -> None:
        (source / ".github").copy(destination.path, dirs_exist_ok=True)


class GitLabProvider(CIProvider):
    NAME = CIProviderChoices.GITLAB.name

    @classmethod
    def cleanup(cls, root_dir: SecurePath):
        (root_dir / ".gitlab-ci.yml").unlink(missing_ok=True)

    def has_template(self, root_dir: SecurePath) -> bool:
        return (root_dir / ".gitlab-ci.yml").exists()

    def copy(self, source: SecurePath, destination: SecurePath) -> None:
        if (destination / ".gitlab-ci.yml").exists():
            (destination / ".gitlab-ci.yml").unlink()
        (source / ".gitlab-ci.yml").move(destination.path)


class CIProviderManager:
    @staticmethod
    def project_post_gen_cleanup(
        selected_provider: Optional[CIProvider], template_root: SecurePath
    ):
        for provider_cls in CIProvider.all():
            if selected_provider and not isinstance(selected_provider, provider_cls):
                provider_cls.cleanup(template_root)
