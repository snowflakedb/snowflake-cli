---
name: upgrade-package
description: >-
  Use when upgrading a Python dependency in the CLI repo — e.g. "upgrade
  snowflake-connector-python to 4.7.2", "bump <package> to <version>",
  "upgrade package SNOW-1234567", or "update dependency". Handles the full
  workflow: Artifactory availability check, vulnerability scan, branch
  creation, pyproject.toml + RELEASE-NOTES.md edits, lock-file regeneration,
  unit tests, commit, and PR creation.
---

# Upgrading a Python dependency

This skill automates the full dependency-upgrade workflow. Follow each phase
in order; do not skip phases.

## Phase 0 — gather inputs

Collect from the user (prompt for anything missing):

- **Ticket number** — e.g. `SNOW-1234567`
- **Package name** — e.g. `snowflake-connector-python`
- **Target version** — e.g. `4.7.2`

---

## Phase 1 — version availability and vulnerability check

### 1a. Classify the package

Determine whether the package is **Snowflake-owned**: its name starts with
`snowflake-` or is in the `snowflake.*` namespace (e.g.
`snowflake-connector-python`, `snowflake-snowpark-python`, `snowflake.core`).

| Package type | Artifactory check |
|---|---|
| Snowflake-owned | **Skip steps 1b and 1c.** Snowflake controls the release pipeline for these packages, so Artifactory propagation lag is expected and not a blocker. Set the confirmed version to the requested version and proceed to 1d. |
| Third-party | Run steps 1b and 1c as normal. |

### 1b. Determine Artifactory URL *(third-party packages only)*

```sh
pip config list | grep index-url
```

If the output contains an `index-url` value, use that. Otherwise fall back to:
```
https://artifactory.ci1.us-west-2.aws-dev.app.snowflake.com/artifactory/api/pypi/development-pypi-virtual/simple
```

### 1c. Query available versions and decide *(third-party packages only)*

```sh
pip index versions <package> --index-url <artifactory_url>
```

**This step must succeed.** If it fails for any reason (network error, auth
failure, unknown package), show the user the exact error output and stop — do
not proceed to Phase 2.

Parse the `Available versions:` line from the output to get the full sorted
list of known versions.

| Condition | Action |
|-----------|--------|
| Target version is not in the available list | Warn that the version is not yet synced to Artifactory and stop |
| Newer versions exist on the **same major** | Ask the user: proceed with the newer version or stick with the originally requested one? |
| Target is available, no newer same-major | Confirm the target version and proceed |

The **confirmed version** (agreed upon here) is used for all subsequent steps.

### 1d. Vulnerability check

Query the OSV database for the confirmed version:

```sh
curl -sf "https://api.osv.dev/v1/query" \
  -H "Content-Type: application/json" \
  -d "{\"version\": \"<confirmed_version>\", \"package\": {\"name\": \"<package>\", \"ecosystem\": \"PyPI\"}}"
```

| Result | Action |
|--------|--------|
| Response contains a non-empty `vulns` array | List each vulnerability ID and summary, warn the user, and stop. Do not continue without explicit user override. |
| Request fails (network/API error) | Warn that the check could not be completed and ask the user whether to continue anyway |
| `vulns` is empty or absent | Proceed |

---

## Phase 2 — branch, file edits, lock regeneration

### 2a. Detect current version

Grep `pyproject.toml` for the line containing `<package>` (with or without extras
like `[secure-local-storage]`) followed by `==`. Extract the version string as
`<old_version>`.

### 2b. Compute branch alias

```sh
NAME=$(git config user.name)
ALIAS=$(echo "$NAME" | awk '{printf tolower(substr($1,1,1)) tolower(substr($NF,1,1))}')
```

Example: "Jakub Maciej Wilkowski" → `jw`.

### 2c. Create branch

```sh
git checkout -b ${ALIAS}/SNOW-<ticket>-upgrade-<package-slug>-<confirmed_version>
```

`<package-slug>` is the bare package name with hyphens, no extras bracket
(e.g. `snowflake-connector-python`).

### 2d. Edit `pyproject.toml`

On the line matching `<package>` (possibly with `[extras]`) and `==<old_version>`,
replace the version in-place:

```
==<old_version>  →  ==<confirmed_version>
```

Preserve the extras bracket unchanged.

### 2e. Edit `RELEASE-NOTES.md`

Under `# Unreleased version` → `## Fixes and improvements`, append a new bullet
**at the end of the existing list** (after all other bullets already present):

```
* Upgraded <package> from <old_version> to <confirmed_version>.
```

### 2f. Regenerate lock files

```sh
hatch run lock-dependencies   # rewrites pylock.toml via uv pip compile --index https://pypi.org/simple
hatch run sync-dependencies   # rewrites snyk/requirements.txt
```

Both commands target public PyPI (`--index https://pypi.org/simple`), not
Artifactory. Verify that the new `pylock.toml` references `files.pythonhosted.org`
URLs, not Artifactory.

---

## Phase 3 — run unit tests

```sh
hatch run test
```

For **Snowflake-owned packages**, hatch will try to sync the virtual environment
from Artifactory before running tests and fail if the new version is not yet
propagated there. Override the index:

```sh
UV_INDEX_URL=https://pypi.org/simple hatch run test
```

If any tests fail: show the failure output to the user and stop. Do not commit
a broken state.

---

## Phase 4 — commit

```sh
git add pyproject.toml RELEASE-NOTES.md pylock.toml snyk/requirements.txt
git commit -m "chore: [SNOW-<ticket>] upgrade <package> to <confirmed_version>"
```

---

## Phase 5 — create PR

### 5a. Fetch upstream release notes

1. Look up the package's source URL from PyPI:
   ```sh
   curl -s "https://pypi.org/pypi/<package>/json" | python3 -c \
     "import json,sys; d=json.load(sys.stdin); urls=d['info'].get('project_urls') or {}; print(urls)"
   ```

2. If a GitHub source URL is present (key `Source`, `Homepage`, or similar
   containing `github.com/<owner>/<repo>`), fetch the release notes for each
   version between `<old_version>` (exclusive) and `<confirmed_version>`
   (inclusive):
   ```sh
   gh release view "v<version>" --repo <owner>/<repo> --json tagName,body
   ```
   Collect the `body` field per release and concatenate them, newest last.

3. If no GitHub URL is available, use the `Changelog` URL from `project_urls`
   if present, or note: _"No release notes URL found — see upstream package
   changelog."_

### 5b. Create the PR

```sh
gh pr create \
  --repo snowflake-eng/snowflake-cli \
  --title "chore: [SNOW-<ticket>] upgrade <package> to <confirmed_version>" \
  --body "<body>"
```

Build the body by reading the checklist from `.github/pull_request_template.md`
and appending two sections after it:

```markdown
<contents of .github/pull_request_template.md>

### Changes description
Upgraded `<package>` from `<old_version>` to `<confirmed_version>`.

### Upstream release notes (`<old_version>` → `<confirmed_version>`)

<fetched release notes content — one section per intermediate version, or the fallback note if unavailable>
```

---

## Reference

| Resource | Path / Command |
|----------|----------------|
| pyproject.toml dependencies | `[project] dependencies` array, line ~45 |
| Release notes | `RELEASE-NOTES.md` — `# Unreleased version` → `## Fixes and improvements` |
| Regenerate pylock.toml | `hatch run lock-dependencies` |
| Regenerate snyk/requirements.txt | `hatch run sync-dependencies` |
| Unit tests | `hatch run test` |
| PR template checklist | `.github/pull_request_template.md` |
| OSV vulnerability API | `https://api.osv.dev/v1/query` |
