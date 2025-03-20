import enum
from typing import Generator, List, Optional, Type

from snowflake.cli.api.secure_path import SecurePath


class CIProviderChoices(str, enum.Enum):
    GITHUB = "GITHUB"
    GITLAB = "GITLAB"


class CIProvider:
    name: str
    files_to_render_directories: list[str] = []

    @classmethod
    def cleanup(cls, root: SecurePath) -> None:
        raise NotImplementedError()

    def __eq__(self, other) -> bool:
        return self.name == other.name

    @classmethod
    def from_choice(cls, choice: CIProviderChoices) -> "CIProvider":
        return {
            CIProviderChoices.GITHUB: GithubProvider,
            CIProviderChoices.GITLAB: GitLabProvider,
        }[choice]()

    @classmethod
    def all(cls) -> List[Type["CIProvider"]]:  # noqa: A003
        return [GithubProvider, GitLabProvider]

    def get_files_to_render(self, root_dir: SecurePath) -> Generator[str, None, None]:
        for directory in self.files_to_render_directories:
            for path in (root_dir / directory).rglob("*"):
                yield str(path.relative_to(root_dir.path))

    def has_template(self, root_dir: SecurePath) -> bool:
        raise NotImplementedError()


class GithubProvider(CIProvider):
    name = CIProviderChoices.GITLAB.name
    files_to_render_directories = [".github/workflows/"]

    @classmethod
    def cleanup(cls, root_dir: SecurePath):
        (root_dir / ".github").rmdir(recursive=True)

    def has_template(self, root_dir: SecurePath) -> bool:
        return (root_dir / ".github/workflows").exists()


class GitLabProvider(CIProvider):
    name = CIProviderChoices.GITLAB.name

    @classmethod
    def cleanup(cls, root_dir: SecurePath):
        (root_dir / ".gitlab-ci.yml").unlink(missing_ok=True)

    def get_files_to_render(self, root_dir: SecurePath):
        if (root_dir / ".gitlab-ci.yml").exists():
            return [".gitlab-ci.yml"]

    def has_template(self, root_dir: SecurePath) -> bool:
        return (root_dir / ".gitlab-ci.yml").exists()


class CIProviderManager:
    @staticmethod
    def project_post_gen_cleanup(
        selected_provider: Optional[CIProvider], template_root: SecurePath
    ):
        for provider_cls in CIProvider.all():
            if selected_provider and not isinstance(selected_provider, provider_cls):
                provider_cls.cleanup(template_root)
