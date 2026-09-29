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

# Publish command docs

Release helper: regenerate command-reference MDX with `snow --docs-pages` and
copy mapped pages into a local `snowflake-prod-docs` checkout.

Only commands listed in `scripts/command_docs_paths.yaml` are copied.

## Prerequisites

- An environment that can run `snow --docs-pages` (for example Hatch)
- A local clone of `snowflake-eng/snowflake-prod-docs`

## Usage

From the CLI repo root:

```bash
hatch run python scripts/publish_command_docs.py --docs-repo <path_to_snowflake-prod-docs>
```

Alternatively, set the docs repo with an environment variable and omit `--docs-repo`:

```bash
export SNOWFLAKE_PROD_DOCS=<path_to_snowflake-prod-docs>
hatch run python scripts/publish_command_docs.py
```

`--docs-repo` wins if both are set.

Flags:

- `--dry-run` — generate and print planned copies; do not write the docs repo
- `--mapping PATH` — override `scripts/command_docs_paths.yaml`
