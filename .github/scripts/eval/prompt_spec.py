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

"""Static extraction of the reviewer's prompt spec — stdlib only, no external deps.

Used by :mod:`compare` (A/B eval diff) and :mod:`check_changed` (CI pre-check)
to read ``AGENT_PROMPT_TEMPLATE``, ``LOCAL_DIFF_INSTRUCTIONS``, and
``CORTEX_MODEL`` from reviewer-script source without importing or executing it.
The extraction is ``ast``-based so it works on the *baseline* file (a different
git ref) and on the working tree without pulling in Snowflake imports.
"""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass

# Repo-relative paths to the two files that together define the production
# reviewer spec.  Shared here so compare.py and check_changed.py stay in sync.
REVIEWER_REL_PATH = ".github/scripts/cortex_e2e_review.py"
WORKFLOW_REL_PATH = ".github/workflows/cortex_review.yaml"

# The module-level string constants in the reviewer script that together define
# the prompt sent to the Cortex agent.
PROMPT_CONSTANTS = ("AGENT_PROMPT_TEMPLATE", "LOCAL_DIFF_INSTRUCTIONS")

# Fallback model when nothing else resolves.  Must equal runner.DEFAULT_MODEL.
_DEFAULT_MODEL = "claude-opus-4-6"


@dataclass
class PromptSpec:
    """The inputs that distinguish one reviewer run from another."""

    template: str
    diff_instructions: str
    model: str

    def prompt_hash(self) -> str:
        """Short stable digest of the prompt text (template + diff instructions).

        Model is deliberately excluded — it is reported separately, so two specs
        that share a prompt but differ only in model show the same hash.
        """
        h = hashlib.sha256()
        h.update(self.template.encode("utf-8"))
        h.update(b"\x1f")
        h.update(self.diff_instructions.encode("utf-8"))
        return h.hexdigest()[:12]


def _module_string_constants(source: str, names: tuple[str, ...]) -> dict[str, str]:
    """Extract top-level ``NAME = <string literal>`` assignments from *source*.

    Uses ``ast`` (never imports/executes the module — the baseline file version
    may differ from the working tree and pulls in Snowflake imports we don't want
    to trigger).  Adjacent-string-literal concatenation (as
    ``LOCAL_DIFF_INSTRUCTIONS`` uses) is folded by the parser into a single
    ``Constant``, so ``literal_eval`` resolves it directly.
    """
    tree = ast.parse(source)
    wanted = set(names)
    found: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in wanted:
                try:
                    value = ast.literal_eval(node.value)
                except (ValueError, SyntaxError):
                    continue
                if isinstance(value, str):
                    found[target.id] = value
    return found


def _extract_model_default(source: str) -> str | None:
    """Find the ``os.environ.get("CORTEX_MODEL", <default>)`` default in *source*.

    This is the model the reviewer uses when ``CORTEX_MODEL`` is unset — i.e.
    the workflow default.  Returns ``None`` if the pattern isn't found (caller
    falls back to :data:`_DEFAULT_MODEL`).
    """
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "get"
            and isinstance(func.value, ast.Attribute)
            and func.value.attr == "environ"
            and isinstance(func.value.value, ast.Name)
            and func.value.value.id == "os"
        ):
            continue
        if (
            len(node.args) >= 2
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "CORTEX_MODEL"
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            return node.args[1].value
    return None


def _extract_model_from_workflow(source: str) -> str | None:
    """Find the ``CORTEX_MODEL`` env value in the reviewer's workflow YAML.

    The workflow's ``env:`` block is the authoritative production value — it is
    what the CI job actually sets, independent of the script's own fallback.
    Strips surrounding quotes so both ``claude-opus-4-6`` and
    ``"claude-opus-4-6"`` resolve to the same string.
    """
    m = re.search(r"(?m)^\s+CORTEX_MODEL:\s+(.+?)\s*$", source)
    return m.group(1).strip("'\"") if m else None


def extract_prompt_spec(
    source: str,
    workflow_source: str | None = None,
    model_override: str | None = None,
) -> PromptSpec:
    """Build a :class:`PromptSpec` from reviewer-script *source*.

    Model resolution order:

    1. *model_override* (explicit CLI flag)
    2. ``CORTEX_MODEL`` from the workflow YAML (*workflow_source*) — the
       production value the CI job actually uses
    3. ``os.environ.get("CORTEX_MODEL", …)`` default in the reviewer script
    4. :data:`_DEFAULT_MODEL`
    """
    consts = _module_string_constants(source, PROMPT_CONSTANTS)
    missing = [name for name in PROMPT_CONSTANTS if name not in consts]
    if missing:
        raise ValueError(
            f"reviewer source is missing required constant(s): {', '.join(missing)}"
        )
    model = (
        model_override
        or (workflow_source and _extract_model_from_workflow(workflow_source))
        or _extract_model_default(source)
        or _DEFAULT_MODEL
    )
    return PromptSpec(
        template=consts["AGENT_PROMPT_TEMPLATE"],
        diff_instructions=consts["LOCAL_DIFF_INSTRUCTIONS"],
        model=model,
    )
