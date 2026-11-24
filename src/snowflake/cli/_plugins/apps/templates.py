# Copyright (c) 2026 Snowflake Inc.
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

"""Project templates for Snowflake App Runtime apps.

The templates live in the public ``snowflakedb/sar-sdk`` repository on GitHub.
Its ``templates/catalog.json`` lists each template's metadata and the path,
size, and SHA-256 digest of every file a new project gets. The templates release
tag, or the ref the user passes, is resolved to a commit first, so the catalog
and the files always come from the same commit, and every file is checked
against the catalog before it is used.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlparse

import requests
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from snowflake.cli.__about__ import VERSION
from snowflake.cli._plugins.apps.app_yml import APP_YML_FILENAME
from snowflake.cli._plugins.apps.generate import _yaml_str
from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.config import is_air_gapped
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal
from snowflake.cli.api.secure_path import SecurePath
from urllib3.exceptions import NameResolutionError

log = logging.getLogger(__name__)

TEMPLATES_REPOSITORY = "snowflakedb/sar-sdk"
TEMPLATES_URL = f"https://github.com/{TEMPLATES_REPOSITORY}/tree/main/templates"
CATALOG_PATH = "templates/catalog.json"
SUPPORTED_CATALOG_VERSION = 1
# The tag the templates are read from unless --ref says otherwise. Moving it
# releases templates that are already on main, or rolls a release back. There
# is one tag per catalog version, so this CLI never reads a release in a
# catalog format it doesn't know.
TEMPLATES_RELEASE_TAG = f"templates-v{SUPPORTED_CATALOG_VERSION}"

GITHUB_URL = "https://github.com"
GITHUB_RAW_URL = "https://raw.githubusercontent.com"

# Seconds to wait for a connection, and then for each read.
REQUEST_TIMEOUT = (10, 30)
# Upper bounds that keep a broken response from filling memory.
MAX_REFS_BYTES = 1024 * 1024
MAX_CATALOG_BYTES = 1024 * 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
DOWNLOAD_THREADS = 8

# Telemetry counters, sent with the command's result or error event:
#   snowflake_app.templates.result.<result>   1 for "ok", or for why the templates
#       couldn't be read. "ok" drops to 0 when a later request fails.
#   snowflake_app.templates.failed_request.<request>   1 for the request that
#       failed: "commit" (github.com), or "catalog" or "file"
#       (raw.githubusercontent.com)
#   snowflake_app.templates.http_status   the HTTP status of that request
#   snowflake_app.templates.custom_ref   1 when --ref replaced the release tag
#   snowflake_app.init.template.<id>   1 for the template snow app init used
RESULT_COUNTER = "snowflake_app.templates.result.{}"
FAILED_REQUEST_COUNTER = "snowflake_app.templates.failed_request.{}"
HTTP_STATUS_COUNTER = "snowflake_app.templates.http_status"
CUSTOM_REF_COUNTER = "snowflake_app.templates.custom_ref"
TEMPLATE_COUNTER = "snowflake_app.init.template.{}"

# What ``snow app init`` writes as the first line of app.yml.
PROVENANCE_PREFIX = "# created-by:"
PROVENANCE_CREATOR = "snow-app-init"

_COMMIT = re.compile(r"[0-9a-f]{40}")
# A tag or branch name: git's rules, narrowed to what's safe to print and to
# write into app.yml.
_REF = re.compile(r"(?!.*\.\.)(?!.*//)(?!.*\.lock$)[A-Za-z0-9][A-Za-z0-9._/-]{0,199}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_TEMPLATE_ID = re.compile(r"[a-z][a-z0-9-]*")
_PLACEHOLDER = re.compile(r"<%([A-Z0-9_]+)%>")

_NETWORK_FAILURES = {
    "dns": "the host name could not be resolved",
    "proxy": "the proxy failed the request",
    "tls": "the TLS connection could not be verified",
    "timeout": "the request timed out",
    "connection": "the connection failed",
}


def _is_safe_relative_path(path: str) -> bool:
    return (
        bool(path)
        and "\\" not in path
        and ":" not in path
        and all(part not in ("", ".", "..") for part in path.split("/"))
    )


class TemplateFile(BaseModel):
    model_config = ConfigDict(extra="ignore")

    path: str
    size: int = Field(ge=0, le=MAX_FILE_BYTES)
    sha256: str

    @field_validator("path")
    @classmethod
    def _check_path(cls, value: str) -> str:
        if not _is_safe_relative_path(value):
            raise ValueError(f"unsafe file path {value!r}")
        return value

    @field_validator("sha256")
    @classmethod
    def _check_sha256(cls, value: str) -> str:
        if not _SHA256.fullmatch(value):
            raise ValueError("not a SHA-256 hex digest")
        return value


class Template(BaseModel):
    # Unknown fields are ignored, so a catalog with new fields still loads.
    model_config = ConfigDict(extra="ignore")

    id: str  # noqa: A003
    path: str
    title: str
    summary: str
    when_to_use: str = ""
    when_not_to_use: str = ""
    runtime: str
    framework: str = ""
    requires: List[str] = Field(default_factory=list)
    surfaces: List[str] = Field(default_factory=list)
    default: bool = False
    digest: str = ""
    files: List[TemplateFile]

    @field_validator("id")
    @classmethod
    def _check_id(cls, value: str) -> str:
        if not _TEMPLATE_ID.fullmatch(value):
            raise ValueError(f"invalid template id {value!r}")
        return value

    @field_validator("path")
    @classmethod
    def _check_path(cls, value: str) -> str:
        if not _is_safe_relative_path(value):
            raise ValueError(f"unsafe template path {value!r}")
        return value

    @field_validator("files")
    @classmethod
    def _check_files(cls, value: List[TemplateFile]) -> List[TemplateFile]:
        paths = [file.path for file in value]
        if len(set(paths)) != len(paths):
            raise ValueError("duplicate file paths")
        if APP_YML_FILENAME not in paths:
            raise ValueError(f"no {APP_YML_FILENAME}")
        return value


class Catalog(BaseModel):
    model_config = ConfigDict(extra="ignore")

    catalog_version: int
    templates: List[Template]

    @field_validator("templates")
    @classmethod
    def _check_templates(cls, value: List[Template]) -> List[Template]:
        ids = [template.id for template in value]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate template ids")
        return value


class TemplateSourceError(CliError):
    """The templates could not be read from GitHub."""


class _RequestError(Exception):
    """A failed request, with what telemetry records about it.

    Download threads raise it, and the command's thread records it, because the
    CLI context belongs to that thread.
    """

    def __init__(self, result: str, message: str, http_status: Optional[int] = None):
        super().__init__(message)
        self.result = result
        self.message = message
        self.http_status = http_status


def _failure(
    result: str,
    message: str,
    request: Optional[str] = None,
    http_status: Optional[int] = None,
) -> TemplateSourceError:
    """Record why the templates couldn't be read, and return the error to raise."""
    metrics = get_cli_context().metrics
    if metrics.get_counter(RESULT_COUNTER.format("ok")) is not None:
        metrics.set_counter(RESULT_COUNTER.format("ok"), 0)
    metrics.set_counter(RESULT_COUNTER.format(result), 1)
    if request is not None:
        metrics.set_counter(FAILED_REQUEST_COUNTER.format(request), 1)
    if http_status is not None:
        metrics.set_counter(HTTP_STATUS_COUNTER, http_status)
    return TemplateSourceError(message)


def _caused_by_name_resolution(error: BaseException) -> bool:
    """Whether a failed request's chain of causes includes a DNS lookup failure."""
    pending: List[Any] = [error]
    seen = set()
    while pending:
        current = pending.pop()
        if not isinstance(current, BaseException) or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (NameResolutionError, socket.gaierror)):
            return True
        pending += [
            current.__cause__,
            current.__context__,
            getattr(current, "reason", None),
            *current.args,
        ]
    return False


def _network_failure(error: requests.RequestException) -> str:
    # Most specific first: proxy and TLS errors are connection errors too.
    if isinstance(error, requests.exceptions.ProxyError):
        return "proxy"
    if isinstance(error, requests.exceptions.SSLError):
        return "tls"
    if isinstance(error, requests.exceptions.Timeout):
        return "timeout"
    if _caused_by_name_resolution(error):
        return "dns"
    return "connection"


def _advertised_refs(advertisement: bytes) -> Optional[Dict[str, str]]:
    """The refs in a git smart-HTTP ref advertisement, or None if it's malformed.

    The advertisement is a series of pkt-lines: four hex digits giving the
    line's length, including those digits, then ``<object> <ref>``. The first
    ref's line also carries capabilities after a NUL byte. ``0000`` is a flush.
    An annotated tag appears twice: as ``refs/tags/<name>`` with the tag object,
    and as ``refs/tags/<name>^{}`` with the commit it points to.
    """
    if not advertisement:
        return None
    refs: Dict[str, str] = {}
    position = 0
    while position < len(advertisement):
        try:
            length = int(advertisement[position : position + 4], 16)
        except ValueError:
            return None
        if length == 0:
            position += 4
            continue
        if length < 4 or position + length > len(advertisement):
            return None
        line = advertisement[position + 4 : position + length]
        position += length
        if line.startswith(b"#"):
            continue
        sha, _, name = line.split(b"\0", 1)[0].rstrip(b"\n").partition(b" ")
        sha_text = sha.decode("ascii", "replace")
        if _COMMIT.fullmatch(sha_text):
            refs[name.decode("utf-8", "replace")] = sha_text
    return refs


@dataclass(frozen=True)
class TemplateSource:
    """The template catalog at one commit of the templates repository."""

    ref: str
    commit: str
    catalog: Catalog

    @property
    def templates(self) -> List[Template]:
        """The templates, the default first and then by id."""
        return sorted(
            self.catalog.templates,
            key=lambda template: (not template.default, template.id),
        )

    def get(self, template_id: Optional[str]) -> Template:
        """The template with *template_id*, or the default template when it is None."""
        for template in self.templates:
            if template.id == template_id or (template_id is None and template.default):
                return template
        available = ", ".join(template.id for template in self.templates)
        if template_id is None:
            raise CliError(
                f"The template catalog has no default template. Pass --template "
                f"with one of: {available}."
            )
        raise CliError(
            f"Unknown template '{sanitize_for_terminal(template_id)}'. Available "
            f"templates: {available}. Run `snow app templates` to compare them."
        )


class TemplateRepository:
    """Reads templates from the templates repository on GitHub.

    *ref* is a tag, a branch, or a full commit SHA. Without one, the templates
    come from the release tag.
    """

    def __init__(self, ref: Optional[str] = None) -> None:
        if ref is not None and not (_COMMIT.fullmatch(ref) or _REF.fullmatch(ref)):
            raise CliError(
                f"'{sanitize_for_terminal(ref)}' isn't a tag, a branch, or a full "
                "commit SHA. Pass one with --ref."
            )
        self.ref = ref or TEMPLATES_RELEASE_TAG
        self._custom_ref = ref is not None
        # requests doesn't promise that a session is thread-safe, so each
        # download thread gets its own.
        self._local = threading.local()
        self._sessions: List[requests.Session] = []
        self._sessions_lock = threading.Lock()

    def __enter__(self) -> TemplateRepository:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        for session in self._sessions:
            session.close()

    def load(self) -> TemplateSource:
        """Resolve the ref to a commit and read the catalog at that commit."""
        if self._custom_ref:
            get_cli_context().metrics.set_counter(CUSTOM_REF_COUNTER, 1)
        if is_air_gapped():
            raise _failure(
                "air_gapped",
                "Snowflake App Runtime templates are read from GitHub, and "
                "cli.air_gapped is set. Copy a template from "
                f"{TEMPLATES_URL} on a machine with internet access instead.",
            )
        commit = self._resolve_commit()
        catalog = self._read_catalog(commit)
        get_cli_context().metrics.set_counter(RESULT_COUNTER.format("ok"), 1)
        return TemplateSource(ref=self.ref, commit=commit, catalog=catalog)

    def download(
        self, source: TemplateSource, template: Template, destination: SecurePath
    ) -> None:
        """Write *template*'s files into *destination*, once all match the catalog."""
        get_cli_context().metrics.set_counter(TEMPLATE_COUNTER.format(template.id), 1)
        paths = [f"{template.path}/{file.path}" for file in template.files]
        with ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS) as pool:
            downloads = [
                pool.submit(
                    self._get, self._raw_url(source.commit, path), MAX_FILE_BYTES
                )
                for path in paths
            ]
            try:
                contents = [download.result() for download in downloads]
            except _RequestError as error:
                for download in downloads:
                    download.cancel()
                raise _failure(
                    error.result,
                    error.message,
                    request="file",
                    http_status=error.http_status,
                ) from error
        for file, path, content in zip(template.files, paths, contents):
            if (
                len(content) != file.size
                or hashlib.sha256(content).hexdigest() != file.sha256
            ):
                raise _failure(
                    "integrity",
                    f"{path} at commit {source.commit} of {TEMPLATES_REPOSITORY} "
                    "doesn't match the template catalog, so the project was not "
                    "created. Try again, and report it at "
                    f"https://github.com/{TEMPLATES_REPOSITORY}/issues if it keeps "
                    "happening.",
                    request="file",
                )
        for file, content in zip(template.files, contents):
            target = destination / file.path
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("wb") as fd:
                fd.write(content)

    def _resolve_commit(self) -> str:
        if _COMMIT.fullmatch(self.ref):
            return self.ref
        # The ref list git reads for `git ls-remote`. GitHub's REST API would
        # allow only 60 requests an hour per IP address, shared by everyone
        # behind it.
        url = (
            f"{GITHUB_URL}/{TEMPLATES_REPOSITORY}.git/info/refs"
            "?service=git-upload-pack"
        )
        refs = _advertised_refs(self._fetch("commit", url, MAX_REFS_BYTES))
        if refs is None:
            raise _failure(
                "invalid_response",
                f"{url} didn't return the repository's tags and branches, so the "
                "Snowflake App Runtime templates can't be read. Try again later.",
                request="commit",
            )
        for name in (
            f"refs/tags/{self.ref}^{{}}",
            f"refs/tags/{self.ref}",
            f"refs/heads/{self.ref}",
        ):
            if name in refs:
                return refs[name]
        if self._custom_ref:
            message = (
                f"{TEMPLATES_REPOSITORY} has no tag or branch named {self.ref}. "
                "Pass --ref with a tag, a branch, or a full commit SHA."
            )
        else:
            message = (
                f"{TEMPLATES_REPOSITORY} has no {self.ref} tag yet, so there's no "
                "templates release to read. To use the unreleased templates, "
                "pass --ref main."
            )
        raise _failure("unknown_ref", message, request="commit")

    def _read_catalog(self, commit: str) -> Catalog:
        url = self._raw_url(commit, CATALOG_PATH)
        body = self._fetch("catalog", url, MAX_CATALOG_BYTES)
        try:
            raw = json.loads(body)
        except ValueError:
            raw = None
        version = raw.get("catalog_version") if isinstance(raw, dict) else None
        if isinstance(version, int) and version > SUPPORTED_CATALOG_VERSION:
            raise _failure(
                "unsupported_catalog",
                f"The template catalog at {url} has version {version}, and this "
                f"Snowflake CLI reads version {SUPPORTED_CATALOG_VERSION}. Upgrade "
                "Snowflake CLI to use the templates.",
                request="catalog",
            )
        try:
            return Catalog.model_validate(raw)
        except ValidationError as error:
            log.debug("Invalid template catalog at %s", url, exc_info=True)
            raise _failure(
                "invalid_response",
                f"{url} isn't a valid template catalog, so the Snowflake App "
                "Runtime templates can't be read. Upgrade Snowflake CLI, or try "
                "again later.",
                request="catalog",
            ) from error

    @staticmethod
    def _raw_url(commit: str, path: str) -> str:
        return f"{GITHUB_RAW_URL}/{TEMPLATES_REPOSITORY}/{commit}/{quote(path)}"

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers["User-Agent"] = f"snowflake-cli/{VERSION}"
            self._local.session = session
            with self._sessions_lock:
                self._sessions.append(session)
        return session

    def _fetch(
        self,
        request: str,
        url: str,
        limit: int,
        headers: Optional[Dict[str, str]] = None,
    ) -> bytes:
        """Read *url* on the command's thread, recording a failure for telemetry."""
        try:
            return self._get(url, limit, headers)
        except _RequestError as error:
            raise _failure(
                error.result,
                error.message,
                request=request,
                http_status=error.http_status,
            ) from error

    def _get(
        self, url: str, limit: int, headers: Optional[Dict[str, str]] = None
    ) -> bytes:
        """Read *url*. Safe to call from any thread: it records nothing."""
        host = urlparse(url).hostname
        try:
            with self._session().get(
                url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True
            ) as response:
                if response.status_code != 200:
                    raise self._http_failure(url, response)
                content = bytearray()
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    content += chunk
                    if len(content) > limit:
                        raise _RequestError(
                            "too_large",
                            f"{url} returned more than {limit} bytes, so the "
                            "Snowflake App Runtime templates can't be read.",
                        )
                return bytes(content)
        except requests.RequestException as error:
            failure = _network_failure(error)
            log.debug("Request to %s failed", url, exc_info=True)
            raise _RequestError(
                failure,
                f"Could not reach {host} to read the Snowflake App Runtime "
                f"templates: {_NETWORK_FAILURES[failure]}. The templates come "
                f"from {TEMPLATES_URL}. Check your network and proxy settings, "
                "then try again.",
            ) from error

    @staticmethod
    def _http_failure(url: str, response: requests.Response) -> _RequestError:
        status = response.status_code
        if status == 429 or (
            status == 403 and response.headers.get("X-RateLimit-Remaining") == "0"
        ):
            return _RequestError(
                "rate_limited",
                f"GitHub's rate limit for requests from your network is used up, "
                f"so {url} can't be read now. Try again in an hour.",
                http_status=status,
            )
        if status == 404:
            hint = f"Upgrade Snowflake CLI, or find the templates at {TEMPLATES_URL}."
        elif status >= 500:
            hint = "GitHub may be having problems. Try again later."
        else:
            hint = "Check your network and proxy settings, then try again."
        return _RequestError(
            "http",
            f"{url} returned HTTP {status}, so the Snowflake App Runtime templates "
            f"can't be read. {hint}",
            http_status=status,
        )


def readable_label(name: str) -> str:
    """A display label for an app name, e.g. ``sales-dashboard`` -> ``Sales Dashboard``.

    Matches the label Snowsight Workspaces derives for the projects it creates.
    """
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", re.sub(r"[_-]+", " ", name))
    return " ".join(word[0].upper() + word[1:] for word in spaced.split())


def render_app_yml(
    text: str,
    *,
    template: Template,
    ref: str,
    commit: str,
    name: str,
    label: str,
    database: str,
    schema: str,
    warehouse: str,
    build_eai: Optional[str],
) -> str:
    """Fill in a template's app.yml placeholders, the way Snowsight Workspaces does.

    Adds ``build_eai`` after ``query_warehouse`` when one is set, and makes the
    first line record the template, ref, and commit the project came from.
    """
    values = {
        "APP_NAME": name,
        "DATABASE": _yaml_str(database),
        "SCHEMA": _yaml_str(schema),
        "QUERY_WAREHOUSE": _yaml_str(warehouse),
        # Templates quote the label: '<%LABEL%>'.
        "LABEL": label.replace("'", "''"),
    }
    unknown = sorted(set(_PLACEHOLDER.findall(text)) - set(values))
    if unknown:
        listed = ", ".join(f"<%{placeholder}%>" for placeholder in unknown)
        raise CliError(
            f"The {template.id} template's {APP_YML_FILENAME} uses placeholders "
            f"this Snowflake CLI can't fill in: {listed}. Upgrade Snowflake CLI "
            "and try again."
        )
    text = _PLACEHOLDER.sub(lambda match: values[match.group(1)], text)
    if build_eai and not re.search(r"^build_eai:", text, flags=re.MULTILINE):
        line = f"build_eai: {_yaml_str(build_eai)}"

        def _append_line(match: re.Match[str]) -> str:
            return f"{match.group(1)}\n{line}"

        text, count = re.subn(
            r"^(query_warehouse:.*)$",
            _append_line,
            text,
            count=1,
            flags=re.MULTILINE,
        )
        if count == 0:
            text = f"{text.rstrip()}\n{line}\n"
    provenance = (
        f"{PROVENANCE_PREFIX} {PROVENANCE_CREATOR}; template={template.id}; "
        f"ref={ref}; commit={commit}"
    )
    first, newline, rest = text.partition("\n")
    if first.startswith(PROVENANCE_PREFIX):
        return provenance + newline + rest
    return f"{provenance}\n{text}"
