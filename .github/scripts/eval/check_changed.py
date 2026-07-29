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

"""Detect whether the reviewer's prompt or model changed between a baseline ref
and the working tree.

Called by ``cortex_eval.yaml`` as an inexpensive pre-check before the full A/B
eval run.  Uses only Python stdlib (no Snowflake or TruLens deps) so it can run
in the ``check-changed`` CI job without installing the CLI package first.

Writes ``changed=true|false`` to ``$GITHUB_OUTPUT`` when that env var is set.

Exit codes:
  0 = completed (``changed`` written to GITHUB_OUTPUT)
  1 = could not read baseline from git (missing ref or file)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

# prompt_spec.py is in the same directory — stdlib only, no external deps.
_EVAL_DIR = Path(__file__).resolve().parent
if str(_EVAL_DIR) not in sys.path:
    sys.path.insert(0, str(_EVAL_DIR))

from prompt_spec import (  # noqa: E402
    REVIEWER_REL_PATH,
    WORKFLOW_REL_PATH,
    extract_prompt_spec,
)


def _git_show(ref_path: str) -> str | None:
    """Return the contents of ``<ref>:<path>`` or ``None`` if not found."""
    try:
        return subprocess.check_output(
            ["git", "show", ref_path], text=True, stderr=subprocess.DEVNULL
        )
    except subprocess.CalledProcessError:
        return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=(
            "Check whether the reviewer prompt or model differs between a git ref "
            "and the working tree.  Writes 'changed=true|false' to $GITHUB_OUTPUT."
        )
    )
    p.add_argument(
        "--base-ref",
        default="origin/main",
        help="Baseline git ref to compare against (default: origin/main)",
    )
    args = p.parse_args(argv)

    base_src = _git_show(f"{args.base_ref}:{REVIEWER_REL_PATH}")
    if base_src is None:
        print(
            f"error: could not read {REVIEWER_REL_PATH} from {args.base_ref}",
            file=sys.stderr,
        )
        return 1
    base_wf = _git_show(f"{args.base_ref}:{WORKFLOW_REL_PATH}")

    cand_src = Path(REVIEWER_REL_PATH).read_text()
    cand_wf_path = Path(WORKFLOW_REL_PATH)
    cand_wf = cand_wf_path.read_text() if cand_wf_path.exists() else None

    base_spec = extract_prompt_spec(base_src, base_wf)
    cand_spec = extract_prompt_spec(cand_src, cand_wf)

    changed = (
        base_spec.prompt_hash() != cand_spec.prompt_hash()
        or base_spec.model != cand_spec.model
    )

    print(f"baseline   prompt={base_spec.prompt_hash()}  model={base_spec.model}")
    print(f"candidate  prompt={cand_spec.prompt_hash()}  model={cand_spec.model}")
    print(f"changed: {changed}")

    gh_output = os.environ.get("GITHUB_OUTPUT")
    if gh_output:
        with open(gh_output, "a") as fh:
            fh.write(f"changed={'true' if changed else 'false'}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
