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

from pathlib import Path

from typer import Argument, Typer

EMPTY_RELEASE_NOTES = """\
# Unreleased version
## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

"""

REPO_ROOT = Path(__file__).parent.parent
_UNRELEASED_HEADING = "# Unreleased version"


def rewrite_release_notes(current_content: list[str], new_version: str) -> list[str]:
    """Return RELEASE-NOTES.md lines with Unreleased promoted to ``v{new_version}``."""
    heading_index = next(
        (
            i
            for i, line in enumerate(current_content)
            if line.strip() == _UNRELEASED_HEADING
        ),
        None,
    )
    if heading_index is None:
        raise ValueError(
            "RELEASE-NOTES.md must contain a '# Unreleased version' heading."
        )

    content = list(current_content)
    content[heading_index] = f"# v{new_version}\n"
    return [
        *content[:heading_index],
        *EMPTY_RELEASE_NOTES.splitlines(keepends=True),
        "\n",
        *content[heading_index:],
    ]


def main():
    app = Typer(help="Snowflake CLI dev tool")

    @app.command(hidden=True)
    def _dummy():
        """To force typer to require command"""
        pass

    @app.command()
    def update_release_notes(
        new_version: str = Argument(help="Version being released"),
    ):
        """
        Updates release notes with version being release, prepares template
        for next unreleased version.
        """
        release_notes = REPO_ROOT / "RELEASE-NOTES.md"
        with release_notes.open("r") as fh:
            current_content = fh.readlines()

        new_content = rewrite_release_notes(current_content, new_version)
        with release_notes.open("w+") as fh:
            fh.writelines(new_content)

    app()


if __name__ == "__main__":
    main()
