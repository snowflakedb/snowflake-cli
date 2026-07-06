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

# ADR 0002: Tag mirroring strategy

## Status

Accepted

## Context

The Copybara ITERATIVE workflow mirrors commits from the internal repo to the
public mirror, appending a `GitOrigin-RevId: <src-sha>` trailer to every
commit. Because this rewrites the commit, destination SHAs differ from source
SHAs. A tag in the source repo points to a source SHA that does not exist on
the mirror, so tags cannot be pushed directly.

## Decision

Tags are mirrored by a separate script (`mirror-tags.sh`) that maps source
commit SHAs to their destination equivalents via the `GitOrigin-RevId` trailer,
then creates matching refs on the mirror using the GitHub API.

### Why not Copybara?

Copybara operates on commit history, not Git refs. There is no Copybara
primitive for mirroring tags.

### Annotated tags

Annotated tags carry a message and tagger identity. The GitHub API does not
allow impersonating the original tagger, so annotated tags are recreated as new
tag objects with:
- **Message** preserved from the source tag object.
- **Tagger** set to `Mirror Bot <mirror-bot@snowflake.com>` (same identity used
  for mirrored commits).

Lightweight tags (a ref pointing directly to a commit) are created as-is.

### Tag patterns and branch inference

Two regex patterns cover the known tag formats:

| Pattern | Regex | Branch search order |
|---|---|---|
| Release | `^v[0-9]+\.[0-9]+\.[0-9]+$` | inferred `release-vX.Y.Z` → `main` |
| RC | `^v[0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+$` | `main` → inferred `release-vX.Y.Z` |

The inferred branch is derived from the tag name by stripping any suffix:
`v3.20.0` → `release-v3.20.0`, `v3.20.0-rc1` → `release-v3.20.0`.
This covers the observed tagging practice:
- Final release tags are on their release branch.
- RC tags are on `main` (tagged before the release branch is cut) or on the
  release branch after it is cut.

The search is limited to these two candidates. If the commit is not found on
either, the tag is flagged as an error — this indicates a tagging mistake on
the source, not a missing mirror branch.

### Custom tags (deferred)

Tags not matching the above patterns (e.g. `dcm-early-access`,
`native-pipelines-prpr`) are skipped with a log line. Supporting them requires
a way to infer the source branch without a full branch scan.

**Planned convention (not yet enforced):** custom tags must encode their source
branch in the tag name using a defined separator, so the branch can be
extracted deterministically. The exact naming format is TBD. Until this
convention is established and tooling is updated, custom tags are not mirrored
automatically.
