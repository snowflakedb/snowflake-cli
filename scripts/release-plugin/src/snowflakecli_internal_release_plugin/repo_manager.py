# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager
from functools import cached_property
from pathlib import Path
from typing import Optional

from click import ClickException
from git import Repo
from snowflake.cli.api.console.console import cli_console

from snowflakecli_internal_release_plugin.utils import subprocess_run


class RepositoryManager(Repo):
    """Repository manager."""

    def __init__(self):
        self.home_path = Path(
            subprocess_run(["git", "rev-parse", "--show-toplevel"]).strip()
        )
        super().__init__(self.home_path)
        self.remotes.origin.fetch()

    def exists(self, ref: str) -> bool:
        return any(ref == r.name for r in self.references)

    @contextmanager
    def tmp_checkout(self, ref: str):
        """Contextmanager returning to current branch after execution."""
        current_ref = self.head.reference
        try:
            with cli_console.phase(f"checking out to {ref}"):
                self.git.checkout(ref)
                self.git.pull()
            yield
        finally:
            with cli_console.phase(f"checking out back to {current_ref.name}"):
                self.git.checkout(current_ref.name)


class ReleaseInfo:
    """Class providing information about release."""

    def __init__(self, version, repo: RepositoryManager):
        subprocess_run(["git", "fetch", "--all"])
        self.repo = repo
        self.version = version

    @property
    def release_branch_name(self) -> str:
        return f"release-v{self.version}"

    @property
    def final_tag_name(self) -> str:
        return f"v{self.version}"

    def rc_tag_name(self, number: int) -> str:
        return f"{self.final_tag_name}-rc{number}"

    @cached_property
    def _existing_tags(self):
        return [tag.name for tag in self.repo.tags if self.version in tag.name]

    @cached_property
    def last_released_rc(self) -> Optional[int]:
        last_tag = max(self._existing_tags, default=None)
        if last_tag is None or last_tag == self.final_tag_name:
            return None
        rc_number = last_tag.removeprefix(f"{self.final_tag_name}-rc")
        return int(rc_number)

    @cached_property
    def latest_released_tag(self) -> Optional[str]:
        if self.repo.exists(self.final_tag_name):
            return self.final_tag_name
        if self.last_released_rc is None:
            return None
        return self.rc_tag_name(self.last_released_rc)

    @property
    def next_rc(self) -> int:
        if self.last_released_rc is None:
            return 0
        return self.last_released_rc + 1

    @staticmethod
    def cherrypick_branch_name(tag_name: str) -> str:
        return f"cherrypicks-{tag_name}"

    def check_status(self):
        def _show_branch_if_exists(branch_name: str) -> Optional[str]:
            return branch_name if self.repo.exists(branch_name) else None

        return {
            "version": self.version,
            "version released": self.repo.exists(self.final_tag_name),
            "branch": _show_branch_if_exists(self.release_branch_name),
            "last released tag": self.latest_released_tag,
            "next rc": self.next_rc,
            "next rc cherrypick branch": _show_branch_if_exists(
                self.cherrypick_branch_name(self.rc_tag_name(self.next_rc))
            ),
            "final cherrypicks branch": _show_branch_if_exists(
                self.cherrypick_branch_name(self.final_tag_name)
            ),
        }

    def assert_release_branch_exists(self):
        if not self.repo.exists(self.release_branch_name):
            raise ClickException(
                f"Branch `{self.release_branch_name}` does not exist. Did you call 'snow release init'?"
            )

    def assert_release_branch_not_exists(self):
        if self.repo.exists(self.release_branch_name):
            raise ClickException(f"Branch `{self.release_branch_name}` already exists.")
