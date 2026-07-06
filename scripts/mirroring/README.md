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

# Mirroring: private `snowflake-eng/snowflake-cli` → public `snowflakedb/snowflake-cli`

Development happens private-first in the internal repo. These scripts mirror
internal `main` (and release branches) out to the public OSS repo using
[Copybara](https://github.com/google/copybara) in `ITERATIVE` mode — one
destination commit per origin commit, original author and message preserved.

What gets published and what doesn't is a deliberate policy — see [`adr/`](adr/)
and the `EXCLUDED_PATHS` denylist in
[`copybara/copy.bara.sky`](copybara/copy.bara.sky).

## Layout

| Path | Purpose |
|---|---|
| `copybara/copy.bara.sky` | Copybara config: exclusion denylist and internal-reference guards |
| `copybara/Dockerfile` | Copybara runtime image (pins and checksum-verifies the release jar) |
| `copybara/import_paths.bara.sky` | Placeholder for inbound import; not used by the mirror |
| `lib/config.sh` | The only per-project knob — repos, bot identity, workflow name |
| `lib/copybara-run.sh` | Runs Copybara in Docker; interprets exit codes (`4` = nothing to sync) |
| `mirror-main.sh` | Incremental mirror of `main` (used by CI) |
| `mirror-branch.sh` | Incremental mirror of a release branch; auto-seeds on first run |
| `mirror-release-branches.sh` | Incremental mirror of all `release-v*` branches |
| `mirror-tags.sh` | Mirror `v*` tags; maps source SHAs to destination via `GitOrigin-RevId` |
| `migrate-branch.sh` | One-time bootstrap: establishes the `GitOrigin-RevId` baseline on a new branch |
| `local/` | Local wrappers: mint tokens, build the image, delegate to the core scripts |
| `local/local-mirror-tags.sh` | Local wrapper for `mirror-tags.sh` (no Docker required) |
| `adr/0002-tag-mirroring-strategy.md` | Tag mirroring design decisions and custom tag naming convention |

**`migrate` vs `mirror`:** run `migrate-branch.sh` once on a branch that has
never been mirrored to establish the baseline. After that, `mirror-*.sh` handles
the ongoing incremental sync.

## Running

### CI

| Workflow | Trigger | What it runs |
|---|---|---|
| `mirror-main.yml` | push to `main` + hourly fallback (`7 * * * *`) | `mirror-main.sh` |
| `mirror-release.yml` | `workflow_dispatch` + daily at 20:00 CET (`0 19 * * *` UTC) | `mirror-release-branches.sh` then `mirror-tags.sh` |
| `mirror-custom-branches.yml` | push to allowlisted branches | `mirror-branch.sh` |

`mirror-release.yml` is the intended entry point for the releasing engineer: run it
manually via `workflow_dispatch` when the internal release process is done (branch
cut, cherry-picks, tags all pushed). The daily schedule is a fallback only.

Release branches and tags are intentionally clustered in one workflow — branches
sync first, so tags are guaranteed to find their `GitOrigin-RevId` target.

### Locally
The `local/` wrappers reproduce the CI flow. Prerequisites: `gh`
authenticated to the source org, `docker`, and a `local/.env` with the Mirror Bot
credentials (see [Secrets](#secrets)).

```bash
bash scripts/mirroring/local/local-mirror-main.sh --dry-run
bash scripts/mirroring/local/local-mirror-main.sh

bash scripts/mirroring/local/local-mirror-release-branches.sh --dry-run
bash scripts/mirroring/local/local-mirror-tags.sh --dry-run
```

## Secrets

`local/.env` holds the Mirror Bot GitHub App private key used to mint a push
token for the public repo. It is git-ignored; treat it as a high-value credential
— it grants write access to the public mirror. If it is ever exposed, rotate the
key immediately in the GitHub App settings.

## Decisions

Non-obvious choices — why a denylist, why fail-first guards instead of rewriting,
what's excluded and why — are in [`adr/`](adr/).
