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
"""Deciding whether a failed tenant invocation is worth retrying.

Only a capacity rejection is retryable. Definition errors, missing objects and
permission errors are terminal, and retrying them multiplies load for no gain.

A capacity rejection is recognised by its numeric codes, never by its message
text. The CLI renders errors inside a box and wraps them mid-phrase, so a search
for a phrase like "too many concurrent calls" does not fire against raw stderr:

    ╭─ Error ──────────────────────────────────────────╮
    │ 001617 (53400): Too many concurrent calls to the  │
    │ Jinja2 templating engine: Max number of running   │
    │ operations (5) exceeded for account               │
    ╰──────────────────────────────────────────────────╯

Normalising first (stripping box-drawing characters and collapsing whitespace)
makes the text readable again, but the classification itself still keys off the
codes.
"""

from __future__ import annotations

import logging
import re
from typing import Optional

log = logging.getLogger(__name__)

# Box-drawing block, used by the CLI's error frame.
_BOX_DRAWING = re.compile(r"[\u2500-\u257f]")
_WHITESPACE = re.compile(r"\s+")

# The Snowflake error number and SQL state for a templating-capacity rejection:
# "001617 (53400): Too many concurrent calls to the Jinja2 templating engine".
# Both are matched with a word boundary so an unrelated number that merely
# contains these digits does not count as a rejection.
_CAPACITY_CODES = (
    re.compile(r"(?<!\d)001617(?!\d)"),
    re.compile(r"(?<!\d)53400(?!\d)"),
)

# "Max number of running operations (5) exceeded for account" -- the account's
# enforced ceiling. Parsed rather than hardcoded, because the cap can be raised
# and the enforced value is what the user needs to know.
_CAP_VALUE = re.compile(
    r"max number of running operations\s*\(\s*(\d+)\s*\)", re.IGNORECASE
)


def normalize_error_text(text: Optional[str]) -> str:
    """Undo the CLI's error framing so the message reads as one line.

    Strips box-drawing characters and collapses every whitespace run to a single
    space, which rejoins phrases the box broke across lines.
    """
    if not text:
        return ""
    return _WHITESPACE.sub(" ", _BOX_DRAWING.sub(" ", text)).strip()


def is_capacity_rejection(text: Optional[str]) -> bool:
    """True when the output carries a templating-capacity rejection code."""
    normalized = normalize_error_text(text)
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in _CAPACITY_CODES)


def parse_cap_value(text: Optional[str]) -> Optional[int]:
    """The concurrency ceiling the account reported, if the message states one."""
    match = _CAP_VALUE.search(normalize_error_text(text))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:  # pragma: no cover - the regex only matches digits
        return None


def is_retryable(text: Optional[str]) -> bool:
    """Whether a failed invocation should be retried.

    Capacity rejections are transient and clear on their own. Everything else --
    a definition error, a missing object, a permission error -- is terminal.
    """
    retryable = is_capacity_rejection(text)
    log.debug("Classified failed tenant invocation (retryable=%s).", retryable)
    return retryable
