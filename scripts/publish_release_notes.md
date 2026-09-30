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

# Publish release notes

Writer helper: copy a CLI release version from `RELEASE-NOTES.md` into
`snowflake-prod-docs` (year page, version variable, monthly table row).

Does not run git or open a PR. Use [`publish_prod_docs.py`](publish_prod_docs.md)
for the full release flow.

## Prerequisites

- CLI checkout on the release branch/tag (`VERSION` in `__about__.py` is `X.Y.Z`)
- `monthly-details/{YYYY-MM}.mdx` already exists in prod-docs for the ship month

## Usage

```bash
hatch run python scripts/publish_release_notes.py \
  --docs-repo <path_to_snowflake-prod-docs> \
  --release-date 2026-09-28
```

`--release-date` is required (ship date for the heading, `<Release>`, and monthly
table). `--dry-run` prints planned updates without writing files.
