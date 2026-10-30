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

"""Auto-upgrade decision engine.

``maybe_auto_upgrade`` decides skip / upgrade / fail-open and does not spawn.
``maybe_auto_upgrade_and_spawn`` is the process-start entry: on upgrade it
starts the version-dir binary and exits; otherwise the caller continues.
Download and retarget go through ``evaluate_managed_upgrade`` /
``apply_upgrade`` (no second downloader). The Typer hook lives in
``_app/main_typer.py``.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple, Union

import requests
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade.controls import (
    AUTO_UPGRADE_DONE_ENV,
    allow_major,
    skip_due_to_cli_flag,
)
from snowflake.cli._plugins.upgrade.layout import ManagedLayout, read_reverted_from
from snowflake.cli._plugins.upgrade.lock import try_upgrade_lock
from snowflake.cli._plugins.upgrade.machine import (
    get_or_create_machine_id,
    rollout_score,
)
from snowflake.cli._plugins.upgrade.manager import (
    DOWNLOAD_NOT_AVAILABLE,
    STATUS_ALREADY_CURRENT,
    STATUS_NEW_MAJOR,
    STATUS_REFUSED,
    STATUS_UPGRADED,
    UpgradeDecision,
    evaluate_managed_upgrade,
    get_version_source,
    parse_cli_version,
    set_version_source,
)
from snowflake.cli._plugins.upgrade.repo import HttpRepo
from snowflake.cli._plugins.upgrade.rollout import RolloutPolicy, in_slice
from snowflake.cli._plugins.upgrade.telemetry import TRIGGER_AUTO, record_upgrade_event
from snowflake.cli.api.config import (
    AUTO_UPGRADE_KEY,
    CLI_SECTION,
    get_config_bool_value,
    get_env_value,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.utils.types import try_cast_to_bool

log = logging.getLogger(__name__)

# Check is pointer + signed manifest. Download is the tarball. Manual
# ``snow upgrade`` keeps HttpRepo's longer defaults.
CHECK_TIMEOUT_SECONDS = 15
DOWNLOAD_TIMEOUT_SECONDS = 60

REASON_NOT_DIRECT_INSTALL = "not_direct_install"
REASON_FLAG_OFF = "flag_off"
REASON_CONFIG_OFF = "config_off"
REASON_ENV_OFF = "env_off"
REASON_CLI_FLAG = "cli_flag"
REASON_ALREADY_DONE = "already_done"
REASON_META_COMMAND = "meta_command"
REASON_LOCK_BUSY = "lock_busy"
REASON_NOT_IN_SLICE = "not_in_slice"
REASON_HOLD = "hold"
REASON_NEW_MAJOR = "new_major"
REASON_ALREADY_CURRENT = "already_current"
REASON_TIMEOUT = "timeout"
REASON_NETWORK = "network"
REASON_INTEGRITY = "integrity"
REASON_REVERTED_VERSION = "reverted_version"
REASON_ERROR = "error"

_STATUS_SKIPPED = "skipped"
_META_OPTIONS = frozenset(
    {
        "--help",
        "-h",
        "--version",
        "--info",
        "--docs",
        "--structure",
    }
)
_INTEGRITY_MARKERS = (
    "checksum",
    "signature",
    "refusing to install",
    "refusing tarball",
    "truncated",
)
_NETWORK_MARKERS = ("failed to fetch", "failed to download")
_DECISION_STATUSES = frozenset(
    {STATUS_ALREADY_CURRENT, STATUS_NEW_MAJOR, STATUS_REFUSED, STATUS_UPGRADED}
)


class _AutoTimeoutError(Exception):
    """The auto-upgrade check or download exceeded its budget."""


@dataclass(frozen=True)
class Skip:
    reason: str
    rollout_score: Optional[float] = None
    decision: Optional[UpgradeDecision] = None


@dataclass(frozen=True)
class Upgraded:
    decision: UpgradeDecision
    rollout_score: Optional[float] = None


@dataclass(frozen=True)
class FailedOpen:
    reason: str
    rollout_score: Optional[float] = None


AutoUpgradeAction = Union[Skip, Upgraded, FailedOpen]


def maybe_auto_upgrade(
    argv: Optional[Iterable[str]] = None,
) -> AutoUpgradeAction:
    """Decide whether this process should auto-upgrade.

    ``argv`` is the user arguments (no program name). ``None`` is empty;
    this does not read ``sys.argv``. Tokens after ``--agent-args`` are
    ignored here: that tail belongs to the agent, so ``--help`` there is not
    a Snow meta invocation. Never raises. Does not spawn.
    """
    started = time.monotonic()
    tokens = _argv_before_agent_args(list(argv) if argv is not None else [])
    try:
        action = _decide(tokens)
    except Exception:
        log.debug("Auto-upgrade failed open", exc_info=True)
        action = FailedOpen(REASON_ERROR)
    _record(action, started)
    return action


def maybe_auto_upgrade_and_spawn(argv: Optional[Iterable[str]] = None) -> None:
    """Auto-upgrade, then spawn the new binary when the apply succeeded.

    ``argv`` is the user arguments (no program name), the same list the
    original command will see. The decision ignores the ``--agent-args`` tail;
    ``run_new_binary`` receives this uncut list. Skip and fail-open return so
    that command still runs. A successful apply calls ``run_new_binary``, which
    does not return. Spawn errors (missing binary, ``subprocess`` failure) are
    fail-open: this process continues on the old binary. Does not raise.
    """
    tokens = list(argv) if argv is not None else []
    try:
        action = maybe_auto_upgrade(tokens)
        if not isinstance(action, Upgraded):
            return
        version = action.decision.payload.get("to")
        if not isinstance(version, str) or not version:
            log.debug("Auto-upgrade apply had no target version; continuing")
            return
        # Local import: spawn is only needed after a successful apply.
        # ``tokens`` stays uncut so the child sees the agent tail.
        from snowflake.cli._plugins.upgrade.spawn import run_new_binary

        run_new_binary(version, tokens)
    except Exception:
        log.debug("Auto-upgrade spawn failed open", exc_info=True)


def _decide(tokens: list[str]) -> AutoUpgradeAction:
    if __about__.INSTALLATION_SOURCE != CLIInstallationSource.SNOWFLAKE_MANAGED:
        return Skip(REASON_NOT_DIRECT_INSTALL)

    opt_out = _opt_out_reason(tokens)
    if opt_out is not None:
        return Skip(opt_out)

    if _is_meta(tokens):
        return Skip(REASON_META_COMMAND)

    layout = ManagedLayout()
    with try_upgrade_lock(layout) as acquired:
        if not acquired:
            return FailedOpen(REASON_LOCK_BUSY)
        return _decide_locked(layout)


def _decide_locked(layout: ManagedLayout) -> AutoUpgradeAction:
    score = rollout_score(get_or_create_machine_id(layout))
    source = get_version_source()
    previous_timeouts = _tighten_http_timeouts(source)
    try:
        fetched = _fetch_release(source, score)
        if not isinstance(fetched, tuple):
            return fetched
        release, score = fetched
        version = getattr(release, "version", None)
        rollout = getattr(release, "rollout", None)
        if not isinstance(version, str) or not version or rollout is None:
            return FailedOpen(REASON_ERROR, rollout_score=score)
        if rollout.policy is RolloutPolicy.HOLD:
            return Skip(REASON_HOLD, rollout_score=score)
        if not in_slice(score, rollout):
            return Skip(REASON_NOT_IN_SLICE, rollout_score=score)
        if _blocked_by_revert(version, read_reverted_from(layout)):
            return Skip(REASON_REVERTED_VERSION, rollout_score=score)
        _begin_download_budget(source)
        return _upgrade(source, version, score)
    finally:
        _restore_http_timeouts(source, previous_timeouts)


def _upgrade(source: object, version: str, score: float) -> AutoUpgradeAction:
    deadline = time.monotonic() + DOWNLOAD_TIMEOUT_SECONDS
    pinned = _PinnedVersionSource(version, source, deadline)
    previous = get_version_source()
    set_version_source(pinned)
    try:
        try:
            decision = evaluate_managed_upgrade(
                dry_run=False, allow_major=allow_major()
            )
        except _AutoTimeoutError:
            return FailedOpen(REASON_TIMEOUT, rollout_score=score)
        except requests.Timeout:
            return FailedOpen(REASON_TIMEOUT, rollout_score=score)
        except requests.RequestException:
            return FailedOpen(REASON_NETWORK, rollout_score=score)
        except CliError as exc:
            return FailedOpen(_classify_cli_error(exc), rollout_score=score)
    finally:
        set_version_source(previous)

    status = str(decision.payload.get("status", ""))
    if status == STATUS_UPGRADED:
        return Upgraded(decision, rollout_score=score)
    if status == STATUS_NEW_MAJOR:
        return Skip(REASON_NEW_MAJOR, rollout_score=score, decision=decision)
    if status == STATUS_ALREADY_CURRENT:
        return Skip(REASON_ALREADY_CURRENT, rollout_score=score, decision=decision)
    if status == STATUS_REFUSED:
        return Skip(REASON_NOT_DIRECT_INSTALL, rollout_score=score, decision=decision)
    return FailedOpen(REASON_ERROR, rollout_score=score)


def _fetch_release(
    source: object, score: float
) -> Union[AutoUpgradeAction, Tuple[object, float]]:
    latest_release = getattr(source, "latest_release", None)
    if not callable(latest_release):
        log.debug("Version source has no latest_release; failing open")
        return FailedOpen(REASON_ERROR, rollout_score=score)

    started = time.monotonic()
    try:
        release = latest_release()
    except _AutoTimeoutError:
        return FailedOpen(REASON_TIMEOUT, rollout_score=score)
    except requests.Timeout:
        return FailedOpen(REASON_TIMEOUT, rollout_score=score)
    except requests.RequestException:
        return FailedOpen(REASON_NETWORK, rollout_score=score)
    except CliError as exc:
        return FailedOpen(_classify_cli_error(exc), rollout_score=score)
    except Exception:
        log.debug("Auto-upgrade check failed open", exc_info=True)
        return FailedOpen(REASON_ERROR, rollout_score=score)

    if time.monotonic() - started > CHECK_TIMEOUT_SECONDS:
        return FailedOpen(REASON_TIMEOUT, rollout_score=score)
    return (release, score)


class _PinnedVersionSource:
    """Feed ``evaluate_managed_upgrade`` the release we already fetched.

    Avoids a second pointer request. The download budget is enforced around
    ``materialize``, before ``apply_upgrade`` retargets the shim.
    """

    def __init__(self, version: str, inner: object, deadline: float) -> None:
        self._version = version
        self._inner = inner
        self._deadline = deadline

    def latest_version(self) -> str:
        return self._version

    def materialize(self, version: str, layout: ManagedLayout):
        if time.monotonic() > self._deadline:
            raise _AutoTimeoutError()
        materialize = getattr(self._inner, "materialize", None)
        if not callable(materialize):
            raise CliError(DOWNLOAD_NOT_AVAILABLE)
        installed = materialize(version, layout)
        if time.monotonic() > self._deadline:
            raise _AutoTimeoutError()
        return installed


def _tighten_http_timeouts(
    source: object,
) -> Optional[Tuple[float, float, Optional[float]]]:
    if not isinstance(source, HttpRepo):
        return None
    previous = (
        source.pointer_timeout,
        source.download_timeout,
        source.check_deadline,
    )
    source.pointer_timeout = CHECK_TIMEOUT_SECONDS
    source.download_timeout = DOWNLOAD_TIMEOUT_SECONDS
    source.check_deadline = time.monotonic() + CHECK_TIMEOUT_SECONDS
    return previous


def _restore_http_timeouts(
    source: object, previous: Optional[Tuple[float, float, Optional[float]]]
) -> None:
    if previous is None or not isinstance(source, HttpRepo):
        return
    source.pointer_timeout, source.download_timeout, source.check_deadline = previous


def _begin_download_budget(source: object) -> None:
    """Give the tarball its own 60s budget after the 15s check."""
    if not isinstance(source, HttpRepo):
        return
    source.pointer_timeout = DOWNLOAD_TIMEOUT_SECONDS
    source.download_timeout = DOWNLOAD_TIMEOUT_SECONDS
    source.check_deadline = time.monotonic() + DOWNLOAD_TIMEOUT_SECONDS


def _opt_out_reason(tokens: list[str]) -> Optional[str]:
    """Precedence: done, one-run flag, env, config, feature flag."""
    if _env_is_true(AUTO_UPGRADE_DONE_ENV):
        return REASON_ALREADY_DONE
    if skip_due_to_cli_flag(tokens):
        return REASON_CLI_FLAG

    env_raw = get_env_value(CLI_SECTION, key=AUTO_UPGRADE_KEY)
    if env_raw not in (None, ""):
        if not _env_says_on(env_raw):
            return REASON_ENV_OFF
        if not FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE.is_enabled():
            return REASON_FLAG_OFF
        return None

    if (
        get_config_bool_value(CLI_SECTION, key=AUTO_UPGRADE_KEY, default=False)
        is not True
    ):
        return REASON_CONFIG_OFF
    if not FeatureFlag.ENABLE_SNOW_AUTO_UPGRADE.is_enabled():
        return REASON_FLAG_OFF
    return None


def _env_is_true(name: str) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return False
    return _env_says_on(raw)


def _env_says_on(raw: str) -> bool:
    """True for ``true`` / ``1`` / ``yes`` / ``on``. Anything else is off.

    ``try_cast_to_bool`` rejects ``yes`` and ``on`` with ``ValueError``. Letting
    that escape makes an opt-in look like an engine failure (``FailedOpen``).
    """
    if raw.strip().lower() in {"yes", "on"}:
        return True
    try:
        return try_cast_to_bool(raw) is True
    except ValueError:
        return False


def _argv_before_agent_args(tokens: list[str]) -> list[str]:
    """Drop the agent tail. ``--help`` after ``--agent-args`` is not Snow's.

    Returns a new list. Callers that spawn must keep the original argv.
    """
    try:
        cut = tokens.index("--agent-args")
    except ValueError:
        return tokens
    return tokens[:cut]


def _is_meta(tokens: list[str]) -> bool:
    """``snow upgrade`` and root diagnostics. Empty argv (REPL) is not meta.

    ``--format`` is not consulted; JSON invocations still upgrade.

    Click treats ``--flag value`` and ``--flag=value`` as the same option, so
    both spellings skip. That includes a subcommand ``--version`` such as
    ``snow app publish --version v1``.
    """
    if _first_command(tokens) == "upgrade":
        return True
    return any(_is_meta_token(token) for token in tokens)


def _is_meta_token(token: str) -> bool:
    if token in _META_OPTIONS:
        return True
    name, sep, _value = token.partition("=")
    return bool(sep) and name in _META_OPTIONS


def _first_command(tokens: list[str]) -> Optional[str]:
    for token in tokens:
        if token == "--":
            continue
        if token.startswith("-"):
            continue
        return token
    return None


def _blocked_by_revert(latest: str, stamp: Optional[str]) -> bool:
    """Skip while the published pointer is not newer than the reverted version."""
    if not stamp:
        return False
    try:
        return parse_cli_version(latest) <= parse_cli_version(stamp)
    except CliError:
        log.debug("Ignoring unreadable reverted-from stamp", exc_info=True)
        return False


def _classify_cli_error(exc: CliError) -> str:
    text = str(exc).lower()
    if "timeout" in text or "timed out" in text:
        return REASON_TIMEOUT
    if any(marker in text for marker in _INTEGRITY_MARKERS):
        return REASON_INTEGRITY
    if any(marker in text for marker in _NETWORK_MARKERS):
        return REASON_NETWORK
    return REASON_ERROR


def _record(action: AutoUpgradeAction, started: float) -> None:
    if isinstance(action, Skip) and action.reason == REASON_NOT_DIRECT_INSTALL:
        return
    duration_ms = int((time.monotonic() - started) * 1000)
    decision = action.decision if isinstance(action, (Skip, Upgraded)) else None
    from_version = None
    to_version = None
    if decision is not None:
        from_version = decision.payload.get("from")
        to_version = decision.payload.get("to")
    record_upgrade_event(
        trigger=TRIGGER_AUTO,
        status=_status_for(action),
        duration_ms=duration_ms,
        from_version=from_version if isinstance(from_version, str) else None,
        to_version=to_version if isinstance(to_version, str) else None,
        skip_reason=None if isinstance(action, Upgraded) else action.reason,
        rollout_score=action.rollout_score,
    )


def _status_for(action: AutoUpgradeAction) -> str:
    if isinstance(action, Upgraded):
        status = action.decision.payload.get("status")
        if isinstance(status, str) and status:
            return status
        return STATUS_UPGRADED
    if isinstance(action, Skip) and action.decision is not None:
        status = action.decision.payload.get("status")
        if isinstance(status, str) and status in _DECISION_STATUSES:
            return status
    return _STATUS_SKIPPED
