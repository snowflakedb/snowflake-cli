# Copyright (c) 2026 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Command-reference docs for ``snow bundle``, attached in ``interface.py``."""

from __future__ import annotations

from snowflake.cli.api.commands.command_docs import (
    PUBLIC_PREVIEW,
    CommandDocs,
    Example,
    RelatedLink,
    bullet,
    bullet_list,
    code,
    link,
    note,
    plain_text,
    ref,
)

_CODE_BUNDLES_GUIDE = "/developer-guide/code-bundles/code-bundles"
_CODE_BUNDLE_YML = "/developer-guide/code-bundles/code-bundle-yml-reference"


def _related(*extra: RelatedLink) -> tuple[RelatedLink, ...]:
    return (
        link(_CODE_BUNDLES_GUIDE, "Snowflake Code Bundles"),
        *extra,
        link("/developer-guide/snowflake-cli/index"),
        link(
            "/developer-guide/snowflake-cli/command-reference/overview",
            "Snowflake CLI command reference",
        ),
    )


CREATE_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(
        link("/sql-reference/sql/create-code-bundle", "CREATE CODE BUNDLE"),
        link(_CODE_BUNDLE_YML, "code_bundle.yml reference"),
    ),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle create"),
            " command creates a code bundle by running ",
            code("CREATE CODE BUNDLE"),
            ". The ",
            code("--source"),
            " option accepts the following source types:",
        ),
        bullet_list(
            bullet(
                "A stage path that starts with ",
                code("@"),
                "; for example, ",
                code("@my_stage/project"),
                ".",
            ),
            bullet(
                "A workspace path that starts with ",
                code("snow://workspace/"),
                ".",
            ),
            bullet(
                "A local directory, given as a path with no protocol prefix or "
                "with the ",
                code("file://"),
                " prefix. ",
                ref("sf-cli"),
                " creates a temporary stage, uploads the directory recursively, "
                "and creates the code bundle from that stage.",
            ),
        ),
        plain_text(
            "Place a ",
            code("code_bundle.yml"),
            " file in the root of the source directory to configure how the "
            "code bundle runs.",
        ),
        plain_text(
            "Use ",
            code("--exclude"),
            " to skip files or directories when uploading a local directory. "
            "Each pattern is matched against every path component, so ",
            code("venv"),
            " excludes any file or directory named ",
            code("venv"),
            " at any depth, and ",
            code("*.pyc"),
            " excludes all ",
            code(".pyc"),
            " files. You can specify ",
            code("--exclude"),
            " multiple times. The option is ignored for stage and workspace "
            "sources.",
        ),
        note(
            "The ",
            code("--overwrite"),
            " and ",
            code("--skip-if-exists"),
            " options are mutually exclusive.",
        ),
    ),
    examples=(
        Example(
            description=plain_text(
                "Create a code bundle from a local directory, excluding the ",
                code("venv"),
                " directory:",
            ),
            command='snow bundle create MY_BUNDLE --source ./my_project --exclude "venv"',
        ),
        Example(
            description=plain_text(
                "Create or replace a code bundle from files on a stage:"
            ),
            command="snow bundle create MY_BUNDLE --source @MY_STAGE/project --overwrite",
        ),
        Example(
            description=plain_text("Create a code bundle from a workspace:"),
            command=(
                "snow bundle create MY_BUNDLE "
                '--source \'snow://workspace/"USER$"."PUBLIC"."DEFAULT$"/versions/live\''
            ),
        ),
    ),
)

LIST_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(
        link("/sql-reference/sql/show-code-bundles", "SHOW CODE BUNDLES"),
    ),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle list"),
            " command lists code bundles by running ",
            code("SHOW CODE BUNDLES"),
            ". Without a scope option, it lists code bundles in the current " "schema.",
        ),
        plain_text(
            "Use ",
            code("--in database <name>"),
            " or ",
            code("--in schema <name>"),
            " to list code bundles in a specific database or schema, or ",
            code("--in-account"),
            " to list code bundles across the account. You can't combine ",
            code("--in"),
            " with ",
            code("--in-account"),
            ".",
        ),
    ),
    examples=(
        Example(
            description=plain_text("List code bundles in the current schema:"),
            command="snow bundle list",
        ),
        Example(
            description=plain_text(
                "List code bundles whose names start with ",
                code("SALES"),
                ":",
            ),
            command='snow bundle list --like "SALES%"',
        ),
        Example(
            description=plain_text("List code bundles in a specific schema:"),
            command="snow bundle list --in schema my_db.my_schema",
        ),
        Example(
            description=plain_text("List code bundles across the account:"),
            command="snow bundle list --in-account",
        ),
    ),
)

DELETE_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle delete"),
            " command drops a code bundle by running ",
            code("DROP CODE BUNDLE"),
            ". Use ",
            code("--if-exists"),
            " to succeed without an error when the code bundle does not exist.",
        ),
    ),
    examples=(
        Example(
            description=plain_text("Drop a code bundle:"),
            command="snow bundle delete MY_BUNDLE",
        ),
        Example(
            description=plain_text("Drop a code bundle only if it exists:"),
            command="snow bundle delete MY_BUNDLE --if-exists",
        ),
    ),
)

ALTER_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle alter"),
            " command runs ",
            code("ALTER CODE BUNDLE"),
            " to do one of the following:",
        ),
        bullet_list(
            bullet(
                code("--rename-to"),
                " renames the code bundle.",
            ),
            bullet(
                code("--add-version"),
                " adds a new version of the code bundle from a stage path "
                "(starting with ",
                code("@"),
                ") or a workspace path (starting with ",
                code("snow://workspace/"),
                "). Each version is an immutable snapshot of the bundle files.",
            ),
        ),
        note(
            "Specify exactly one of ",
            code("--rename-to"),
            " or ",
            code("--add-version"),
            ". Unlike ",
            code("snow bundle create"),
            ", ",
            code("--add-version"),
            " does not accept a local directory.",
        ),
    ),
    examples=(
        Example(
            description=plain_text("Add a new version of a code bundle from a stage:"),
            command="snow bundle alter MY_BUNDLE --add-version @MY_STAGE/updated_project",
        ),
        Example(
            description=plain_text("Rename a code bundle:"),
            command="snow bundle alter MY_BUNDLE --rename-to MY_RENAMED_BUNDLE",
        ),
    ),
)

EXECUTE_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(
        link("/sql-reference/sql/execute-code-bundle", "EXECUTE CODE BUNDLE"),
        link(_CODE_BUNDLE_YML, "code_bundle.yml reference"),
    ),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle execute"),
            " command runs a code bundle at the file given by ",
            code("--entrypoint"),
            " by running ",
            code("EXECUTE CODE BUNDLE"),
            ". By default, the command waits for the execution to finish.",
        ),
        plain_text(
            "Arguments after ",
            code("--"),
            " are passed to the code bundle as its command-line arguments. "
            "Your code can read them with standard library tools such as ",
            code("sys.argv"),
            " or ",
            code("argparse"),
            ".",
        ),
        plain_text(
            "Use ",
            code("--async"),
            " to submit the execution and return its query ID immediately. "
            "Pass that query ID to ",
            code("snow bundle status"),
            " to check progress or to ",
            code("snow bundle cancel"),
            " to stop the execution.",
        ),
        plain_text(
            "Use ",
            code("--overrides"),
            " to pass the path of a local YAML file that replaces the stored ",
            code("code_bundle.yml"),
            " for this execution only. The file is sent with ",
            code("WITH SPECIFICATION"),
            " and must not contain ",
            code("$$"),
            ".",
        ),
        plain_text(
            "Use ",
            code("--execution-name"),
            " to name the execution so you can filter on it with ",
            code("snow bundle history --execution-name"),
            ".",
        ),
    ),
    examples=(
        Example(
            description=plain_text("Execute a code bundle:"),
            command="snow bundle execute MY_BUNDLE --entrypoint main.py",
        ),
        Example(
            description=plain_text("Pass arguments to the code bundle:"),
            command=(
                "snow bundle execute MY_BUNDLE --entrypoint jobs/main.py "
                "-- --source-table RAW_SALES --output-table SALES_AGG"
            ),
        ),
        Example(
            description=plain_text(
                "Execute a code bundle asynchronously and return its query ID:"
            ),
            command="snow bundle execute MY_BUNDLE --entrypoint main.py --async",
            output="Request submitted. Query ID: 01c51743-c819-4261-0000-5349586311aa",
        ),
        Example(
            description=plain_text(
                "Execute a code bundle with a replacement configuration file:"
            ),
            command=(
                "snow bundle execute MY_BUNDLE --entrypoint main.py "
                "--overrides ./code_bundle.dev.yml"
            ),
        ),
    ),
)

STATUS_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle status"),
            " command returns the status of an execution started with ",
            code("snow bundle execute --async"),
            "; for example, ",
            code("RUNNING"),
            ", ",
            code("SUCCESS"),
            ", or ",
            code("FAILED_WITH_ERROR"),
            ".",
        ),
    ),
    examples=(
        Example(
            description=plain_text("Check the status of an async execution:"),
            command="snow bundle status 01c51743-c819-4261-0000-5349586311aa",
            output="Query 01c51743-c819-4261-0000-5349586311aa: RUNNING",
        ),
    ),
)

CANCEL_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(
        link("/sql-reference/functions/system_cancel_query", "SYSTEM$CANCEL_QUERY"),
    ),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle cancel"),
            " command cancels an execution started with ",
            code("snow bundle execute --async"),
            " by calling ",
            code("SYSTEM$CANCEL_QUERY"),
            ".",
        ),
    ),
    examples=(
        Example(
            description=plain_text("Cancel an async execution:"),
            command="snow bundle cancel 01c51743-c819-4261-0000-5349586311aa",
        ),
    ),
)

HISTORY_DOCS = CommandDocs(
    banners=(PUBLIC_PREVIEW,),
    related=_related(),
    usage_notes=(
        plain_text(
            "The ",
            code("snow bundle history"),
            " command returns code bundle executions from the ",
            code("SNOWFLAKE.INFORMATION_SCHEMA.CODE_BUNDLE_HISTORY"),
            " table function. All options are optional filters. Without "
            "filters, the command returns up to 100 recent executions.",
        ),
        bullet_list(
            bullet(
                code("--bundle-name"),
                " accepts a fully qualified name or a bare name. Combine a bare "
                "name with ",
                code("--bundle-database"),
                " and ",
                code("--bundle-schema"),
                ".",
            ),
            bullet(
                code("--start-time-range-start"),
                " and ",
                code("--start-time-range-end"),
                " accept a timestamp string; for example, ",
                code("2026-07-01"),
                ". Both bounds are inclusive.",
            ),
            bullet(
                code("--bundle-types"),
                " accepts ",
                code("custom"),
                ", ",
                code("spark"),
                ", and ",
                code("ml"),
                ". ",
                code("--compute-types"),
                " accepts ",
                code("warehouse"),
                " and ",
                code("compute_pool"),
                ". ",
                code("--language-types"),
                " accepts ",
                code("python"),
                ", ",
                code("java"),
                ", and ",
                code("scala"),
                ". You can specify each of these options multiple times.",
            ),
            bullet(
                code("--status"),
                " accepts a single value: ",
                code("pending"),
                ", ",
                code("running"),
                ", ",
                code("done"),
                ", ",
                code("failed"),
                ", ",
                code("cancelled"),
                ", or ",
                code("deleted"),
                ".",
            ),
        ),
    ),
    examples=(
        Example(
            description=plain_text("Show recent executions of all code bundles:"),
            command="snow bundle history",
        ),
        Example(
            description=plain_text(
                "Show the five most recent executions of a code bundle:"
            ),
            command="snow bundle history --bundle-name MY_DB.MY_SCHEMA.MY_BUNDLE --result-limit 5",
        ),
        Example(
            description=plain_text(
                "Show failed executions that started on or after a date:"
            ),
            command=(
                "snow bundle history --status failed "
                "--start-time-range-start 2026-07-01"
            ),
        ),
    ),
)
