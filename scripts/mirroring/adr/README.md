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

# Architecture Decision Records — mirroring

This directory records **why** the mirroring pipeline is shaped the way it is —
the decisions behind [`../copybara/copy.bara.sky`](../copybara/copy.bara.sky) and
the surrounding scripts. Code comments capture *what*; ADRs capture *why* and
*why-not* (the rejected alternatives), so a future maintainer doesn't quietly
undo a deliberate choice.

## ⚠️ These records stay internal

This directory lives under `scripts/mirroring/`, which is in the `EXCLUDED_PATHS`
denylist — so it is **never mirrored to the public repo**. That is intentional:
an ADR about *what we deliberately keep private* is itself internal
meta-information. ADRs here may reference internal orgs, hosts, and tooling
freely.

**Do not move ADRs to a mirrored path** (e.g. the top-level `docs/`). The
`verify_match` guard in `copy.bara.sky` fails the sync on internal references, so
a mirrored ADR naming `snowflake-eng` would break the pipeline — a backstop, not
a substitute for keeping them here.

## Format

Lightweight [Nygard-style](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions/):
**Context · Decision · Consequences · Alternatives considered**. One file per
decision, named `NNNN-kebab-title.md`, numbered sequentially. `Status` is one of
`Proposed`, `Accepted`, `Superseded by NNNN`, or `Deprecated`.

## Index

| ADR | Status | Title |
|---|---|---|
| [0001](0001-mirror-exclusion-policy.md) | Accepted | Mirror exclusion policy & internal-reference guards |
