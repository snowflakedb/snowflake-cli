<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# Publish prod docs

Release helper: run [`publish_command_docs.py`](publish_command_docs.md) and
[`publish_release_notes.py`](publish_release_notes.md), then open a single draft
PR in `snowflake-prod-docs`.

## Prerequisites

- CLI checkout on the release branch/tag (`VERSION` in `__about__.py` is `X.Y.Z`)
- `monthly-details/{YYYY-MM}.mdx` already exists in prod-docs for the ship month
- `git` and `gh` configured for the docs repo

## Usage

```bash
hatch run python scripts/publish_prod_docs.py \
  --docs-repo <path_to_snowflake-prod-docs> \
  --release-date 2026-09-28
```

`--release-date` is required (ship date for release-note headings and the monthly
table).

Flags:

- `--dry-run` — run writers only; no git or PR
- `--no-pr` — write into the docs working tree; no commit or PR
- `--allow-dirty` — run when the docs clone is not clean. The commit includes
  what was already staged and the prod-docs paths this script updates; other
  unstaged edits are not added.
- `--skip-command-docs` — release notes only
- `--skip-release-notes` — command-reference pages only
- `--mapping PATH` — override `scripts/command_docs_paths.yaml`
