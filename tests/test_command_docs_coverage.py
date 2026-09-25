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

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import List

import pytest
from click import Command
from snowflake.cli._app.dev.docs.commands_docs_generator import _command_page_markdown
from snowflake.cli.api.commands.command_docs import has_explicit_command_docs

_SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import publish_command_docs as pcd  # noqa: E402

# Terminal commands that do not get a per-command CommandDocs page in prod-docs.
_COMMAND_DOCS_EXEMPT_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        ("cortex", "search"),
        ("helpers", "clean-installer-path"),
        ("spcs", "service", "remote-build"),
        ("spcs", "service", "remote-build-history"),
        ("spcs", "service", "remote-build-status"),
    }
)

# These commands have prod-docs pages and are in the process of being migrated.
# Temporarily exempt from the check for the duration of the migration.
# Remove a path when its migration PR lands.
_COMMAND_DOCS_MIGRATION_PENDING_PATHS: frozenset[tuple[str, ...]] = frozenset(
    {
        # app
        ("app", "bundle"),
        ("app", "deploy"),
        ("app", "events"),
        ("app", "open"),
        ("app", "publish"),
        ("app", "release-channel", "add-accounts"),
        ("app", "release-channel", "add-version"),
        ("app", "release-channel", "list"),
        ("app", "release-channel", "remove-accounts"),
        ("app", "release-channel", "remove-version"),
        ("app", "release-channel", "set-accounts"),
        ("app", "release-directive", "add-accounts"),
        ("app", "release-directive", "list"),
        ("app", "release-directive", "remove-accounts"),
        ("app", "release-directive", "set"),
        ("app", "release-directive", "unset"),
        ("app", "run"),
        ("app", "setup"),
        ("app", "teardown"),
        ("app", "validate"),
        ("app", "version", "create"),
        ("app", "version", "drop"),
        ("app", "version", "list"),
        # auth
        ("auth", "oidc", "read-token"),
        # connection
        ("connection", "add"),
        ("connection", "generate-jwt"),
        ("connection", "generate-workload-identity-token"),
        ("connection", "list"),
        ("connection", "remove"),
        ("connection", "set-default"),
        ("connection", "test"),
        # cortex
        ("cortex", "extract-answer"),
        ("cortex", "sentiment"),
        ("cortex", "summarize"),
        ("cortex", "translate"),
        # custom-image
        ("custom-image", "validate"),
        # dbt
        ("dbt", "copy"),
        ("dbt", "deploy"),
        ("dbt", "describe"),
        ("dbt", "drop"),
        ("dbt", "list"),
        # dcm
        ("dcm", "create"),
        ("dcm", "deploy"),
        ("dcm", "describe"),
        ("dcm", "drop"),
        ("dcm", "drop-deployment"),
        ("dcm", "list"),
        ("dcm", "list-deployments"),
        ("dcm", "plan"),
        ("dcm", "purge"),
        # git
        ("git", "copy"),
        ("git", "describe"),
        ("git", "drop"),
        ("git", "execute"),
        ("git", "fetch"),
        ("git", "list"),
        ("git", "list-branches"),
        ("git", "list-files"),
        ("git", "list-tags"),
        ("git", "setup"),
        # helpers
        ("helpers", "check-snowsql-env-vars"),
        ("helpers", "check-version"),
        ("helpers", "detect-encoding"),
        ("helpers", "generate-project-schema"),
        ("helpers", "import-snowsql-connections"),
        ("helpers", "v1-to-v2"),
        # init
        ("init",),
        # logs
        ("logs",),
        # notebook
        ("notebook", "create"),
        ("notebook", "deploy"),
        ("notebook", "execute"),
        ("notebook", "get-url"),
        ("notebook", "open"),
        # object
        ("object", "create"),
        ("object", "describe"),
        ("object", "drop"),
        ("object", "list"),
        # snowpark
        ("snowpark", "build"),
        ("snowpark", "deploy"),
        ("snowpark", "describe"),
        ("snowpark", "drop"),
        ("snowpark", "execute"),
        ("snowpark", "list"),
        ("snowpark", "package", "create"),
        ("snowpark", "package", "lookup"),
        ("snowpark", "package", "upload"),
        # spcs
        ("spcs", "compute-pool", "create"),
        ("spcs", "compute-pool", "deploy"),
        ("spcs", "compute-pool", "describe"),
        ("spcs", "compute-pool", "drop"),
        ("spcs", "compute-pool", "list"),
        ("spcs", "compute-pool", "resume"),
        ("spcs", "compute-pool", "set"),
        ("spcs", "compute-pool", "status"),
        ("spcs", "compute-pool", "stop-all"),
        ("spcs", "compute-pool", "suspend"),
        ("spcs", "compute-pool", "unset"),
        ("spcs", "image-registry", "login"),
        ("spcs", "image-registry", "token"),
        ("spcs", "image-registry", "url"),
        ("spcs", "image-repository", "create"),
        ("spcs", "image-repository", "deploy"),
        ("spcs", "image-repository", "drop"),
        ("spcs", "image-repository", "list"),
        ("spcs", "image-repository", "list-images"),
        ("spcs", "image-repository", "list-tags"),
        ("spcs", "image-repository", "url"),
        ("spcs", "service", "create"),
        ("spcs", "service", "deploy"),
        ("spcs", "service", "describe"),
        ("spcs", "service", "drop"),
        ("spcs", "service", "events"),
        ("spcs", "service", "execute-job"),
        ("spcs", "service", "list"),
        ("spcs", "service", "list-containers"),
        ("spcs", "service", "list-endpoints"),
        ("spcs", "service", "list-instances"),
        ("spcs", "service", "list-roles"),
        ("spcs", "service", "logs"),
        ("spcs", "service", "metrics"),
        ("spcs", "service", "resume"),
        ("spcs", "service", "set"),
        ("spcs", "service", "status"),
        ("spcs", "service", "suspend"),
        ("spcs", "service", "unset"),
        ("spcs", "service", "upgrade"),
        # sql
        ("sql",),
        # stage
        ("stage", "copy"),
        ("stage", "create"),
        ("stage", "describe"),
        ("stage", "drop"),
        ("stage", "execute"),
        ("stage", "list"),
        ("stage", "list-files"),
        ("stage", "remove"),
        # streamlit
        ("streamlit", "deploy"),
        ("streamlit", "describe"),
        ("streamlit", "drop"),
        ("streamlit", "execute"),
        ("streamlit", "get-url"),
        ("streamlit", "list"),
        ("streamlit", "logs"),
        ("streamlit", "share"),
    }
)

# Completeness contract for ``docs=``: page.mdx.jinja2 only emits
# ``<RelatedTopics>``, ``## Usage notes``, and ``## Examples`` when CommandDocs
# (or docstring fallbacks) supply content; Syntax/Arguments/Options are always
# present.
_EXPECTED_COMMAND_DOCS_SECTIONS = (
    "<RelatedTopics>",
    "## Syntax",
    "## Arguments",
    "## Options",
    "## Usage notes",
    "## Examples",
)


def _is_command_docs_exempt(path: tuple[str, ...]) -> bool:
    if path in _COMMAND_DOCS_EXEMPT_PATHS:
        return True
    if path in _COMMAND_DOCS_MIGRATION_PENDING_PATHS:
        return True
    # snow dbt execute <subcommand> — one shared overview page in prod-docs.
    return len(path) == 3 and path[:2] == ("dbt", "execute")


def _iter_terminal_commands(
    command: Command, path: List[str] | None = None
) -> Iterator[tuple[tuple[str, ...], Command]]:
    path = path or []
    if getattr(command, "hidden", False):
        return
    if hasattr(command, "commands"):
        for command_name, command_info in command.commands.items():
            yield from _iter_terminal_commands(command_info, [*path, command_name])
    else:
        yield tuple(path), command


@pytest.mark.parametrize(
    ("path", "exempt"),
    [
        (("sql",), True),
        (("dbt", "execute", "run"), True),
        (("dbt", "list"), True),
        (("cortex", "search"), True),
        (("cortex", "complete"), False),
        (("git", "setup"), True),
        (("spcs", "service", "remote-build"), True),
        (("spcs", "service", "list"), True),
    ],
)
def test_is_command_docs_exempt(path, exempt):
    assert _is_command_docs_exempt(path) is exempt


def test_command_docs_coverage(runner, get_click_context):
    # invoke any command to populate app context (plugins registration)
    runner.invoke(["--help"])

    missing = []
    section_errors = []

    for path, command in _iter_terminal_commands(get_click_context().command):
        if has_explicit_command_docs(command):
            page = _command_page_markdown(command, list(path))
            for section in _EXPECTED_COMMAND_DOCS_SECTIONS:
                if section not in page:
                    section_errors.append(
                        f"snow {' '.join(path)}: missing {section} in generated page"
                    )
        elif not _is_command_docs_exempt(path):
            missing.append("snow " + " ".join(path))

    assert not section_errors, "\n".join(section_errors)
    assert not missing, "Commands missing docs=CommandDocs(...):\n" + "\n".join(missing)


def test_command_docs_paths_coverage(runner, get_click_context):
    runner.invoke(["--help"])

    mapping = pcd.load_mapping(pcd.DEFAULT_MAPPING)
    command_page_paths = {
        pcd.command_page_rel_path(path)
        for path, _command in _iter_terminal_commands(get_click_context().command)
    }
    mapped_pages = {source for source, _dest in mapping}

    missing = []
    stale_keys = sorted(mapped_pages - command_page_paths)

    for path, _command in _iter_terminal_commands(get_click_context().command):
        if _is_command_docs_exempt(path):
            continue
        page_key = pcd.command_page_rel_path(path)
        if page_key not in mapped_pages:
            missing.append(f"snow {' '.join(path)} ({page_key.as_posix()})")

    assert not stale_keys, (
        "Stale command_docs_paths.yaml entries (no matching command page):\n"
        + "\n".join(page.as_posix() for page in stale_keys)
    )
    assert (
        not missing
    ), "Commands missing command_docs_paths.yaml entries:\n" + "\n".join(missing)
