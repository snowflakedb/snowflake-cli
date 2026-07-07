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

"""Loader for PR-review eval cases.

Each case is a directory ``cases/<id>/`` containing:

- ``case.toml`` — metadata + ground truth, following the cortex-evals
  ``task.toml`` convention (``[metadata]`` core + a ``[metadata.e2e_review]``
  subsection for our domain fields).
- ``mutate.py`` — exposes ``TARGETS`` (files it edits) and an idempotent
  ``mutate(repo_root)`` that applies the seeded fault.

The ground-truth fields:

- ``expected_verdict`` — PASS / FAIL / SKIP the reviewer should reach.
- ``expected_commands`` — command fragments the reviewer is expected to *run*
  while investigating (checked against the agent's tool trajectory / report by
  the scorer). This is the near-deterministic "did it actually investigate"
  signal.
- ``rubric`` — free-text guidance for the LLM judge only.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]

_EVAL_DIR = Path(__file__).resolve().parent
CASES_DIR = _EVAL_DIR / "cases"

# Sibling modules (e.g. mutation_helpers) must be importable from mutate.py,
# which is loaded dynamically from an arbitrary path.
if str(_EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(_EVAL_DIR))


@dataclass
class Case:
    name: str
    path: Path
    description: str
    difficulty: str
    tags: list[str]
    expected_verdict: str
    expected_breaking_changes: bool
    needs_live_snowflake: bool
    expected_commands: list[str]
    rubric: str
    targets: list[str]
    mutate: Callable[[Path], None]


def _load_mutate_module(case_dir: Path):
    mutate_path = case_dir / "mutate.py"
    spec = importlib.util.spec_from_file_location(
        f"eval_mutate_{case_dir.name}", mutate_path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {mutate_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_case(case_dir: Path) -> Case:
    data = tomllib.loads((case_dir / "case.toml").read_text(encoding="utf-8"))
    meta = data["metadata"]
    gt = meta["e2e_review"]
    module = _load_mutate_module(case_dir)
    mutate = getattr(module, "mutate", None)
    if not callable(mutate):
        raise ValueError(
            f"{case_dir}/mutate.py must define a callable mutate(repo_root)"
        )
    return Case(
        name=meta["name"],
        path=case_dir,
        description=meta.get("description", ""),
        difficulty=meta.get("difficulty", ""),
        tags=list(meta.get("tags", [])),
        expected_verdict=gt["expected_verdict"],
        expected_breaking_changes=bool(gt.get("expected_breaking_changes", False)),
        needs_live_snowflake=bool(gt.get("needs_live_snowflake", False)),
        expected_commands=list(gt.get("expected_commands", [])),
        rubric=gt.get("rubric", ""),
        targets=list(getattr(module, "TARGETS", [])),
        mutate=mutate,
    )


def load_all_cases(cases_dir: Path = CASES_DIR) -> list[Case]:
    return [
        load_case(d)
        for d in sorted(cases_dir.iterdir())
        if d.is_dir() and (d / "case.toml").exists()
    ]
