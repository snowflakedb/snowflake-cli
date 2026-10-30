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
"""Running one tenant as its own `snow dcm` subprocess.

Each tenant goes through the ordinary single-tenant command path, invoked as a
child process rather than in-process. Three things in the existing command code
make a process boundary the right unit for the MVP:

* the working directory is process-global, and parallel workers sharing one
  project directory contend over the same ``out/`` path, so each worker needs its
  own cwd;
* the multi-step progress display is a single live region that one invocation
  owns for its duration;
* a failed tenant must not be able to disturb its siblings, which a separate
  process gives directly.

The project directory itself is passed with ``--from`` and is only ever read, so
the workers share it while each writes into its own temporary cwd.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake.cli._plugins.dcm.fanout.classify import (
    is_retryable,
    normalize_error_text,
    parse_cap_value,
)
from snowflake.cli._plugins.dcm.fanout.targets import FanoutTarget
from snowflake.cli.api.connections import ConnectionContext
from snowflake.cli.api.exceptions import CliError

log = logging.getLogger(__name__)

# Connection settings that are safe to forward on a child's command line.
# Credentials are deliberately absent: an argument vector is visible to anything
# that can list processes, so a run needs a named connection (or an
# environment-provided credential the child inherits) rather than a secret
# passed through argv.
_FORWARDED_CONNECTION_FLAGS = (
    ("connection_name", "--connection"),
    ("account", "--account"),
    ("user", "--user"),
    ("role", "--role"),
    ("warehouse", "--warehouse"),
    ("database", "--database"),
    ("schema", "--schema"),
    ("host", "--host"),
    ("authenticator", "--authenticator"),
    ("private_key_file", "--private-key-file"),
)

# How long the truncated stderr kept per failed tenant may be. Enough to carry a
# boxed Snowflake error, short enough to keep the JSON run log readable.
_ERROR_EXCERPT_LIMIT = 2000


@dataclass
class TenantOutcome:
    """What happened to one tenant across all of its attempts."""

    name: str
    project_name: str
    status: str = "PENDING"
    attempts: int = 0
    duration_seconds: float = 0.0
    operations: Optional[int] = None
    error: Optional[str] = None
    observed_cap: Optional[int] = None
    capacity_rejections: int = 0

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCESS"

    def as_dict(self) -> Dict[str, Any]:
        return {
            "target": self.name,
            "project_name": self.project_name,
            "status": self.status,
            "attempts": self.attempts,
            "duration_seconds": round(self.duration_seconds, 1),
            "operations": self.operations,
            "capacity_rejections": self.capacity_rejections,
            "error": self.error,
        }


@dataclass
class AttemptResult:
    """The result of a single subprocess run."""

    exit_code: int
    duration_seconds: float
    operations: Optional[int] = None
    error: Optional[str] = None
    retryable: bool = False
    observed_cap: Optional[int] = None

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0


@dataclass
class InvocationTemplate:
    """Everything a run holds constant across its tenants."""

    command_name: str
    from_location: Path
    alias: Optional[str] = None
    delta: bool = False
    connection_arguments: List[str] = field(default_factory=list)

    def argv_for(self, target: FanoutTarget) -> List[str]:
        """The full argument vector for one tenant."""
        argv = [
            *_cli_invocation_prefix(),
            "dcm",
            self.command_name,
            "--target",
            target.name,
            "--from",
            str(self.from_location),
        ]
        if self.delta:
            argv.append("--delta")
        if self.alias:
            argv += ["--alias", self.alias]
        for variable in target.variable_arguments():
            argv += ["-D", variable]
        argv += self.connection_arguments
        # The saved artifact is not written reliably by the phased plan UI, so
        # results are read from the JSON envelope on stdout instead.
        argv += ["--format", "json"]
        return argv

    def display_command_for(self, target: FanoutTarget) -> str:
        """A copy-pasteable rendering of one tenant's invocation, for --list-targets."""
        parts = [
            "snow dcm",
            self.command_name,
            "--target",
            target.name,
        ]
        if self.delta:
            parts.append("--delta")
        if self.alias:
            parts += ["--alias", self.alias]
        for variable in target.variable_arguments():
            parts += ["-D", f'"{variable}"']
        parts += ["--from", str(self.from_location)]
        parts += self.connection_arguments
        return " ".join(parts)


def _cli_invocation_prefix() -> List[str]:
    """How to re-invoke this same CLI build as a child process.

    A packaged binary re-executes itself. Otherwise the installed console script
    may not be on PATH (an editable checkout, a venv that was not activated), so
    the module entry point is invoked through the interpreter that is already
    running this process, which pins the child to the same build.
    """
    if getattr(sys, "frozen", False):
        return [sys.executable]
    return [sys.executable, "-m", "snowflake.cli._app"]


def connection_arguments(context: ConnectionContext) -> List[str]:
    """Forward the parent's connection selection to every child.

    A temporary connection is rejected: its credentials live only in the parent's
    arguments, and re-passing them to hundreds of children would put secrets in
    every child's argument vector.
    """
    if context.temporary_connection:
        raise CliError(
            "--fanout cannot be used with --temporary-connection. Fan-out starts one "
            "child process per target and cannot forward a temporary connection's "
            "credentials to them safely. Use a named connection from config.toml instead."
        )

    arguments: List[str] = []
    for attribute, flag in _FORWARDED_CONNECTION_FLAGS:
        value = getattr(context, attribute, None)
        if value:
            arguments += [flag, str(value)]
    return arguments


def parse_operation_count(stdout: str) -> Optional[int]:
    """Read the operation count out of a child's ``--format json`` output.

    The envelope is a single-row, single-column collection whose one value is
    itself a JSON string: ``[{"<column>": "{\\"version\\": ..., \\"changeset\\":
    [...]}"}]``. The column name comes from the server's cursor description, so
    the first value is taken positionally rather than by key. The operation count
    is the length of ``changeset``.
    """
    if not stdout:
        return None
    try:
        envelope = json.loads(stdout)
    except json.JSONDecodeError:
        log.debug("Tenant stdout was not valid JSON; operation count unavailable.")
        return None

    if isinstance(envelope, list):
        if not envelope:
            return None
        envelope = envelope[0]
    if not isinstance(envelope, dict) or not envelope:
        return None

    inner = next(iter(envelope.values()))
    if isinstance(inner, str):
        try:
            inner = json.loads(inner)
        except json.JSONDecodeError:
            log.debug("Tenant result payload was not valid JSON.")
            return None
    if not isinstance(inner, dict):
        return None

    changeset = inner.get("changeset")
    if isinstance(changeset, list):
        return len(changeset)
    return None


def run_attempt(
    template: InvocationTemplate,
    target: FanoutTarget,
    timeout_seconds: Optional[float] = None,
) -> AttemptResult:
    """Run one tenant once, in its own working directory."""
    argv = template.argv_for(target)
    log.info(
        "Starting tenant invocation (target=%s, command=%s).",
        target.name,
        template.command_name,
    )
    started = time.monotonic()

    # A fresh cwd per attempt so that concurrent workers never share an `out/`
    # directory. The project itself is read from `--from`.
    with tempfile.TemporaryDirectory(prefix="snow-dcm-fanout-") as working_directory:
        try:
            completed = subprocess.run(
                argv,
                cwd=working_directory,
                env=os.environ.copy(),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            duration = time.monotonic() - started
            return AttemptResult(
                exit_code=-1,
                duration_seconds=duration,
                error=f"Timed out after {duration:.0f}s.",
                retryable=False,
            )

    duration = time.monotonic() - started
    if completed.returncode == 0:
        return AttemptResult(
            exit_code=0,
            duration_seconds=duration,
            operations=parse_operation_count(completed.stdout),
        )

    # A failing invocation frames its error on stderr, but the CLI also writes
    # some failures to stdout, so both are classified.
    combined = f"{completed.stderr or ''}\n{completed.stdout or ''}"
    normalized = normalize_error_text(combined)
    return AttemptResult(
        exit_code=completed.returncode,
        duration_seconds=duration,
        error=normalized[:_ERROR_EXCERPT_LIMIT] or f"Exited {completed.returncode}.",
        retryable=is_retryable(combined),
        observed_cap=parse_cap_value(combined),
    )
