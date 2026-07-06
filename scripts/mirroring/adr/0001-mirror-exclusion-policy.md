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

# 0001 — Mirror exclusion policy & internal-reference guards

- **Status:** Accepted
- **Date:** 2026-07-02
- **Ticket:** SNOW-3685913
- **Applies to:** [`../copybara/copy.bara.sky`](../copybara/copy.bara.sky)

## Context

The internal `snowflake-eng/snowflake-cli` repo becomes the source of truth;
`snowflakedb/snowflake-cli` (Apache-2.0) becomes a mirror fed by Copybara. The
public repo already carries almost the entire codebase — `src/`, `tests/`,
`docs/`, `.github/`, and so on. Mirroring *maintains* an existing public
surface; it does not open a closed one.

The motivation for going private-first is to keep AI/agent tooling (`.claude/`,
`CLAUDE.md`) and the mirroring machinery itself out of the public repo. The
risk is narrow: prevent that small set of internal-only material from flowing
outward without accidentally dropping everything else.

## Decision

**Default-allow with an explicit denylist.** Mirror everything; exclude only
three categories:

- **AI/agent config** — `.claude/`, `CLAUDE.md`, and similar tooling files
- **Mirroring machinery** — `scripts/mirroring/` and its CI workflows; never
  publish the thing that does the publishing
- **Internal governance** — `CODEOWNERS`, repo metadata, IDE configs
- **Internal release tooling** — `scripts/release-plugin/`; a Snowflake CLI
  plugin that automates internal release steps (PR creation in the internal
  repo, etc.). Has no public-facing functionality and is not referenced by any
  mirrored code.

`AGENTS.md` is **kept public** — it is a vendor-neutral contributor guide with
no internal content. A commented toggle in `copy.bara.sky` allows hiding it
later if needed.

**Fail loudly on internal references; do not rewrite them.** The sync fails if
any mirrored file contains the internal org name or an internal hostname. We
deliberately do **not** use `core.replace` to silently swap them out: a hard
failure surfaces the leak at the source; a silent rewrite hides it and drifts
source from mirror.

## Caveats

- **Exclusions are not retroactive.** Files already on the mirror (e.g. a
  previously-public `CODEOWNERS`) need a one-time manual scrub before going
  live. Verify with `git ls-files` on the mirror first.
- **The denylist needs maintenance** as new internal-only paths are added (e.g.
  `scripts/release-plugin/` was added after the initial decision). The planned
  `snowflake-cli-mirror-privacy` ArcticOwl check is the intended backstop for
  files the denylist doesn't yet know about.
- **The org-name guard can false-trip** if `snowflake-eng` were ever
  legitimately needed in a mirrored file. This was not the case at decision
  time; narrow the regex or exclude that file if it ever arises.

## Alternatives considered

- **Default-deny allowlist** — mirror only an explicit include list.
  Rejected: silently drops every new file that isn't listed — the wrong failure
  mode for a repo that is overwhelmingly public.

- **`core.replace` rewrites** — silently transform internal references to
  public ones. Rejected: masks leaks and creates source/mirror drift; better to
  see what trips the guards and fix it at the source.

- **A public `docs/adr/` location for this record.** Rejected: the record names
  what we keep private and why — it belongs in the excluded tree.

## References

- `scripts/mirroring/copybara/copy.bara.sky` — implementation
- `.github/workflows/mirror-main.yml` — CI entry point
- `scripts/mirroring/README.md` — operational overview & secret handling
