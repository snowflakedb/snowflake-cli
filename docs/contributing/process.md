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

# Contribution Process

## Reporting bugs and requesting features

File a [GitHub Issue](https://github.com/snowflakedb/snowflake-cli/issues).

Use the issue templates — they collect the version, platform, and reproduction
steps needed to act on the report quickly.

## Contributing code

Fork the repo and submit a pull request from your fork. All forks of
`snowflake-cli` are publicly visible on GitHub.

Keep your fork up to date by rebasing onto upstream rather than merging:

```bash
git remote add sfcli https://github.com/snowflakedb/snowflake-cli.git
git fetch sfcli
git checkout <your-branch>
git rebase sfcli/main
```

A maintainer will review and merge your PR after approval using **squash and
merge**. If your PR is approved and all checks pass but you cannot merge,
contact a maintainer via
[GitHub Issues](https://github.com/snowflakedb/snowflake-cli/issues).

## Commit message format

The squash commit title becomes the permanent commit message on `main`. Follow
[Conventional Commits](https://www.conventionalcommits.org/) and include the
ticket number:

```
feat: [SNOW-1234567] add support for X
fix: [SNOW-1234567] correct Y when Z is empty
chore: [SNOW-1234567] bump dependency versions
```

Common types: `feat`, `fix`, `chore`, `docs`, `refactor`, `test`.

## PR checklist

The two items contributors most commonly miss:

**Snapshot files** — any change to CLI output or help text, including hidden
commands, requires regenerating snapshots. See [testing.md](testing.md).

**Release notes** — see [lifecycle.md](lifecycle.md) for what requires an entry
and the exact format. Wrong section or wrong scope will be caught in review.
