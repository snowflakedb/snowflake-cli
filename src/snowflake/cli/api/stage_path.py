from __future__ import annotations

import re
from pathlib import Path, PurePosixPath

from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import (
    to_string_literal,
)

USER_STAGE_PREFIX = "~"


class StagePath:
    def __init__(
        self,
        stage_name: str,
        path: str | PurePosixPath | None = None,
        git_ref: str | None = None,
        trailing_slash: bool = False,
    ):
        self._stage_name = self.strip_stage_prefixes(stage_name)
        self._path = PurePosixPath(path) if path else PurePosixPath(".")

        self._trailing_slash = trailing_slash
        # Check if user stage
        self._is_user_stage = self._stage_name.startswith(USER_STAGE_PREFIX)

        # Setup git information
        self._git_ref = None
        self._is_git_repo = False
        if git_ref:
            self._git_ref = git_ref
            self._is_git_repo = True

    @classmethod
    def get_user_stage(cls) -> StagePath:
        return cls.from_stage_str("~")

    @property
    def stage(self) -> str:
        return self._stage_name

    @property
    def stage_fqn(self) -> FQN:
        return FQN.from_stage(self.stage)

    @property
    def path(self) -> PurePosixPath:
        return self._path

    @property
    def stage_with_at(self) -> str:
        return self.add_at_prefix(self._stage_name)

    def is_user_stage(self) -> bool:
        return self._is_user_stage

    def is_git_repo(self) -> bool:
        return self._is_git_repo

    @property
    def git_ref(self) -> str | None:
        return self._git_ref

    @staticmethod
    def add_at_prefix(text: str):
        if not text.startswith("@"):
            return "@" + text
        return text

    @staticmethod
    def strip_at_prefix(text: str):
        if text.startswith("@"):
            return text[1:]
        return text

    @staticmethod
    def strip_snow_prefix(text: str):
        if text.startswith("snow://"):
            return text[len("snow://") :]
        return text

    @classmethod
    def strip_stage_prefixes(cls, text: str):
        return cls.strip_at_prefix(cls.strip_snow_prefix(text))

    @classmethod
    def from_stage_str(cls, stage_str: str | FQN):
        stage_str = cls.strip_stage_prefixes(str(stage_str))
        parts = stage_str.split("/", maxsplit=1)
        parts = [p for p in parts if p]
        if len(parts) == 2:
            stage_string, path = parts
        else:
            stage_string = parts[0]
            path = None
        return cls(
            stage_name=stage_string, path=path, trailing_slash=stage_str.endswith("/")
        )

    @classmethod
    def from_git_str(cls, git_str: str):
        """
        @configuration_repo / branches/main  / scripts/setup.sql
        @configuration_repo / branches/"foo/main"  / scripts/setup.sql
        """
        repo_name, git_ref, path = cls._split_repo_path(
            cls.strip_stage_prefixes(git_str)
        )
        return cls(
            stage_name=repo_name,
            path=path,
            git_ref=git_ref,
            trailing_slash=git_str.endswith("/"),
        )

    @staticmethod
    def _split_repo_path(git_str: str) -> tuple[str, str, str]:
        parts = []
        slash_index = 0
        skipping_mode = False
        for current_idx, (char, next_char) in enumerate(zip(git_str[:-1], git_str[1:])):
            if not skipping_mode:
                if char != "/":
                    continue

                # Normal split
                parts.append(git_str[slash_index:current_idx])
                slash_index = current_idx + 1

            if next_char == '"':
                skipping_mode = not skipping_mode
            # Add last part
        parts.append(git_str[slash_index:])
        repo_name = parts[0]
        ref = parts[1] + "/" + parts[2]
        path = "/".join(parts[3:]) if len(parts) > 2 else ""
        return repo_name, ref, path

    def absolute_path(self, no_fqn=False, at_prefix=True) -> str:
        stage_name = self._stage_name
        if not self.is_user_stage() and no_fqn:
            stage_name = FQN.from_string(self._stage_name).name

        path = PurePosixPath(stage_name)
        if self.git_ref:
            path = path / self.git_ref
        if not self.is_root():
            path = path / self._path

        str_path = str(path)
        if at_prefix:
            str_path = self.add_at_prefix(str_path)

        if self._trailing_slash:
            return str_path.rstrip("/") + "/"
        return str_path

    def joinpath(self, path: str | Path) -> StagePath:
        if self.is_file():
            raise ValueError("Cannot join path to a file")
        if isinstance(path, Path):
            path = str(PurePosixPath(path))
        return StagePath(
            stage_name=self._stage_name,
            path=PurePosixPath(self._path) / path.lstrip("/"),
            git_ref=self._git_ref,
        )

    def __truediv__(self, path: str):
        return self.joinpath(path)

    def with_stage(self, stage_name: str) -> StagePath:
        """Returns a new path with new stage name"""
        return StagePath(
            stage_name=stage_name,
            path=self._path,
            git_ref=self._git_ref,
        )

    @property
    def parts(self) -> tuple[str, ...]:
        return self._path.parts

    @property
    def name(self) -> str:
        return self._path.name

    def is_dir(self) -> bool:
        return "." not in self.name

    def is_file(self) -> bool:
        return not self.is_dir()

    @property
    def suffix(self) -> str:
        return self._path.suffix

    @property
    def stem(self) -> str:
        return self._path.stem

    @property
    def parent(self) -> StagePath:
        return StagePath(
            stage_name=self._stage_name, path=self._path.parent, git_ref=self._git_ref
        )

    def is_root(self) -> bool:
        return self._path == PurePosixPath(".")

    def root_path(self) -> StagePath:
        if self.is_git_repo():
            return StagePath(stage_name=self._stage_name, git_ref=self._git_ref)
        return StagePath(stage_name=self._stage_name)

    def is_quoted(self) -> bool:
        path = self.absolute_path()
        return path.startswith("'") and path.endswith("'")

    def path_for_sql(self) -> str:
        path = self.absolute_path()
        if not re.fullmatch(r"@([\w./$])+", path):
            return to_string_literal(path)
        return path

    def quoted_absolute_path(self) -> str:
        if self.is_quoted():
            return self.absolute_path()
        return to_string_literal(self.absolute_path())

    def relative_to(self, stage_path: StagePath) -> PurePosixPath:
        return self.path.relative_to(stage_path.path)

    def get_local_target_path(self, target_dir: Path, stage_root: StagePath):
        # Case for downloading @stage/aa/file.py with root @stage/aa
        if self.relative_to(stage_root) == PurePosixPath("."):
            return target_dir
        return (target_dir / self.relative_to(stage_root)).parent

    def __str__(self):
        return self.absolute_path()

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.absolute_path() == other.absolute_path()
