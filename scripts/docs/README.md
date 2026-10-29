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

# Prod-docs publish

Preview command-reference output with `snow --docs` (writes `.mdx` under
`gen_docs/pages/`). That flag is a CLI preview, not a plugin command.

To open a draft PR in `snowflake-prod-docs` after a release tag, run
`snow release publish-docs` from the tagged `X.Y.Z` checkout (`VERSION` in
`__about__.py` is `X.Y.Z`, not a `.dev0` suffix). Do not generate from `main`
or a `cherrypicks-*` helper branch.

```bash
snow release publish-docs \
  --docs-repo <path_to_snowflake-prod-docs> \
  --release-date 2026-09-28
```

Equivalent without the plugin:

```bash
python scripts/docs/publish_prod_docs.py \
  --docs-repo <path_to_snowflake-prod-docs> \
  --release-date 2026-09-28
```

`--docs-repo` is required unless `SNOWFLAKE_PROD_DOCS` is set (the flag wins).
`--release-date` is required (ship date for release-note headings and the
monthly table). Docs team still merges the draft PR. This step does not block
the prod build or Releng.

## Prerequisites

- CLI checkout on the release tag (`VERSION` is `X.Y.Z`)
- `monthly-details/{YYYY-MM}.mdx` already exists in prod-docs for the ship month
- `git` and `gh` configured for the docs repo
- For the plugin command: install and enable `scripts/release-plugin`

## Flags

- `--dry-run` — run writers only; no git or PR
- `--no-pr` — write into the docs working tree; no commit or PR
- `--allow-dirty` — run when the docs clone is not clean. The commit includes
  what was already staged and the prod-docs paths this script updates; other
  unstaged edits are not added.
- `--skip-command-docs` — release notes only
- `--skip-release-notes` — command-reference pages only
- `--mapping PATH` — override `scripts/docs/command_docs_paths.yaml`

## Writer helpers

`publish_command_docs.py` regenerates command-reference MDX with `snow --docs`
and copies mapped pages. `publish_release_notes.py` copies the CLI version from
`RELEASE-NOTES.md` into the year page, version variable, and monthly table.
Neither runs git or opens a PR; use `publish_prod_docs.py` / `snow release
publish-docs` for the full release flow.

Only commands listed in `command_docs_paths.yaml` are copied.
