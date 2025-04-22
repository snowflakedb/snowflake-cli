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

# Snowflake CLI internal release plugin


This plugin is meant to help in Snowflake CLI release process.
The process in short releases all candidates and the final version from
the `release-vX.Y.Z` branch, which is cut of `main` on the
[cut off date](https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/3372351518/Snowflake+CLI+release+calendar).
The branch itself is protected, so all changes (version bumps, cherrypicks)
must be added via PRs from helper branches (`cherrypicks-vX.Y.ZrcN`)
(it also enforces all github checks are passing before release).

The process is explained in more detail on the [confluence page](https://snowflakecomputing.atlassian.net/wiki/spaces/EN/pages/2940602126/Release+process).

## Installation

Install and enable the plugin in the same environment Snowflake CLI is installed:
```
pip install scripts/release-plugin
snow plugin enable release-plugin
```


## Usage

### `snow release init`

Creates three branches:
- `release-vX.Y.Z` with a single commit adjusting the release notes
- `cherrypicks-vX.Y.Z-rc0` basing on `release-vX.Y.Z` with additional commit
  bumping the version to `rc0`. It's meant to be merged into `release-vX.Y.Z`
- `bump-release-notes-3.7.0` basing on `release-vX.Y.Z` with additional commit
  bumping the version to `X.Y+1.Z.dev0`. It's meant to be merged into `main`

```
* (bump-release-notes-X.Y.Z) bump version to X.Y+1.Z.dev0
| * (cherrypicks-vX.Y.Z-rc0) bump version to X.Y.Z-rc0
|/
* (release-vX.Y.Z) adjust release notes
* (main)
```

### `snow release cherrypick-branch`

Creates and publishes `cherrypicks-vX.Y.Z-rcN` branch (or `cherrypicks-vX.Y.Z` if `--final` flag is provided)
with a single commit bumping the version accordingly. The rc number is chosen based on
published tags.


### `snow release tag`

Validates whether the version on `release-vX.Y.Z` branch matches the tag to be published
and publishes the tag.

The tag is chosen based on previously published tags:
* if `--final` flag is passed, the tag is `vX.Y.Z`
* if no tags were released, the tag is `vX.Y.Zrc0`
* if latest tag is `vX.Y.ZrcN`, the tag is `vX.Y.ZrcN+1`


### `snow release validate-pip-installation`

Installs CLI in isolated virtualenv and runs a few basic commands.
CLI is installed from the latest published github tag for the current release.


### `snow release release-notes`

Extracts release notes for current release from `RELEASE-NOTES.md` file.


### `snow release status`

Shows basic info about the current release.
