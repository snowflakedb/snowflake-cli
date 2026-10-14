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

import os
from pathlib import Path
from typing import Any, Sequence

from snowflake.cli.api.config import is_air_gapped
from snowflake.cli.api.exceptions import CliError

_NO_INDEX = "--no-index"
_STRIP_VALUE_FLAGS = frozenset({"-f", "--find-links", "--extra-index-url"})
_STRIP_PREFIXES = ("--find-links=", "--extra-index-url=")


def is_plaintext_http_url(value: str) -> bool:
    return value.lower().startswith("http://")


def reject_plaintext_http_url(value: str, *, what: str) -> None:
    if is_plaintext_http_url(value):
        raise CliError(f"Plaintext http:// {what} are not allowed. Use https://.")


def resolve_pip_index_url(index_url: str | None) -> str | None:
    return index_url or os.environ.get("PIP_INDEX_URL")


def index_url_from_pip_args(args: Sequence[Any]) -> str | None:
    str_args = [str(arg) for arg in args]
    for i, arg in enumerate(str_args):
        if arg in ("-i", "--index-url"):
            if i + 1 < len(str_args):
                return str_args[i + 1]
            return None
        if arg.startswith("--index-url="):
            return arg.split("=", 1)[1]
        if arg == _NO_INDEX:
            return _NO_INDEX
    return None


def looks_like_local_pip_source(value: str) -> bool:
    if value.startswith(("file://", "/", "./", "../")):
        return True
    return Path(value).exists()


def pip_args_need_index(args: Sequence[Any]) -> bool:
    str_args = [str(arg) for arg in args]
    if index_url_from_pip_args(str_args) is not None:
        return False
    if os.environ.get("PIP_INDEX_URL") or os.environ.get("PIP_NO_INDEX"):
        return False
    packages = [arg for arg in str_args if not arg.startswith("-")]
    return any(not looks_like_local_pip_source(pkg) for pkg in packages)


def ensure_https_pip_index(index_url: str | None) -> str | None:
    resolved = resolve_pip_index_url(index_url)
    if resolved and resolved != _NO_INDEX:
        reject_plaintext_http_url(resolved, what="package indexes")
    if is_air_gapped() and not resolved:
        raise CliError(
            "No package index is configured. Pass --index-url or set PIP_INDEX_URL "
            "to an in-region https:// index."
        )
    return resolved


def pip_index_invocation_args(index_url: str | None) -> list[str]:
    """Args that select the single package index for a CLI-spawned pip.

    Air-gapped invocations use ``--isolated`` so pip.conf and extra ``PIP_*``
    index settings cannot add another source. The only supported remote index
    is ``--index-url`` / ``PIP_INDEX_URL``.
    """
    if is_air_gapped():
        args = ["--isolated"]
        if index_url and index_url != _NO_INDEX:
            args.extend(["-i", index_url])
        elif index_url == _NO_INDEX or os.environ.get("PIP_NO_INDEX"):
            args.append(_NO_INDEX)
        return args
    if index_url and index_url != _NO_INDEX:
        return ["-i", index_url]
    return []


def _strip_unsupported_pip_index_flags(args: Sequence[str]) -> list[str]:
    cleaned: list[str] = []
    skip_next = False
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in _STRIP_VALUE_FLAGS:
            skip_next = True
            continue
        if arg.startswith(_STRIP_PREFIXES):
            continue
        cleaned.append(arg)
    return cleaned


def constrain_pip_install_args(args: Sequence[Any]) -> list[str]:
    """Validate index settings and, when air-gapped, pin pip to one index.

    Extra indexes and find-links are dropped rather than parsed. Set
    ``--index-url`` or ``PIP_INDEX_URL`` for offline installs.
    """
    guard_pip_install_args(args)
    str_args = [str(arg) for arg in args]
    if not is_air_gapped():
        return str_args
    found = index_url_from_pip_args(str_args) or os.environ.get("PIP_INDEX_URL")
    cleaned = _strip_unsupported_pip_index_flags(str_args)
    if found and found != _NO_INDEX and index_url_from_pip_args(cleaned) is None:
        cleaned = ["-i", found, *cleaned]
    return ["--isolated", *cleaned]


def guard_pip_install_args(args: Sequence[Any]) -> None:
    found = index_url_from_pip_args(args) or os.environ.get("PIP_INDEX_URL")
    if found and found != _NO_INDEX:
        reject_plaintext_http_url(found, what="package indexes")
    if is_air_gapped() and pip_args_need_index(args):
        raise CliError(
            "No package index is configured. Pass --index-url or set PIP_INDEX_URL "
            "to an in-region https:// index before installing packages in the sandbox."
        )
