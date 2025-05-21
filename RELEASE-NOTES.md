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
# Unreleased version
## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements


# v3.8.2

## Deprecations

## New additions

## Fixes and improvements
* Fix `enable_release_channels` property in application package definition to default to `None` instead of `False`.


# v3.8.1

## Deprecations

## New additions

## Fixes and improvements
* Upgrade message is printed to stderr.
* Fixed `snowflake.core` import issue on newer Python versions

# v3.8.0

## Deprecations

## New additions
* Added support for OAuth tokens.
* Cleaning up the output directory after coping files to the stage for Streamlit, Notebook, SPCS Service and project.
* Added interactive mode for `snow sql`.
* Added support for async SQL queries (`;>` syntax).
* Added support for `!queries`, `!result` and `!abort` commands from SnowSQL.
* Added `artifact_repository`, `artifact_repository_packages` and `resource_constraint` fields to the Snowpark Entity Model to support direct usage of non-anaconda packages.
* Added `--single-transaction` flag to `snow sql`. Wraps BEGIN/COMMIT around statements to execute them as a single transaction, ensuring all commands complete successfully or no change is applied.

## Fixes and improvements
* Fix for deploying Snowpark project using `!=` operator in `requirements.txt`.
* Fix escaping identifiers for `use` commands.
* Move `enable_release_channels` from global feature flag to project level.
* Fixed `snow spcs service metrics` command to accept fully qualified service names.

# v3.7.1

## Deprecations

## New additions

## Fixes and improvements
* Fix certificate connection issues.
* Fix `snow spcs image-registry login` slow query problem.


# v3.7.0

## Deprecations

## New additions
* Added `--prune` flag to `deploy` commands, which removes files that exist in the stage,
  but not in the local filesystem.
* Added `snow logs` command for retrieving and streaming logs from the server.
* Added `snow helper check-snowsql-env-vars` which reports environment variables from SnowSQL with replacements in CLI.

## Fixes and improvements
* Updated MacOS postinstall script to update PATH if snow not exist.


# v3.6.0

## Deprecations

## New additions
* Added support for `!source` command in SQL. This command allows to execute SQL from a local file.

## Fixes and improvements
* Fix for incompatible options in `snow spcs compute-pool` commands didn't raise error.
* Change binary builds to embed whole Python environment.
* Fixed recursive copying to stage for unbalanced directory trees.
* Fixed checking for new CLI version.
* Added file execution log in stage and git commands.

# v3.5.0

## Deprecations

## New additions
* Extended snowflake.yml support for new entities:
  * SPCS compute pool
  * SPCS service
  * SPCS image repository
* Added `snow compute-pool deploy` command using `snowflake.yml` file.
* Added `snow spcs service deploy` command using `snowflake.yml` file.
* Added `snow image-repository deploy` command using `snowflake.yml` file.
* Added support for creating native applications with manifest version 2.

## Fixes and improvements
* Fix handling of date types in `snow sql` command when using JSON for output format

# v3.4.1
## Backward incompatibility

## Deprecations

## New additions

* Add an optional `stage_subdirectory` field to `application package` entity. When specified, application artifacts are uploaded to this subdirectory instead of the root of the application package's stage.
* Add `snow spcs service events` command to retrieve service-specific events:
  * Supports filtering by service name, container name, instance ID, time intervals (`--since`, `--until`), and pagination (`--first`, `--last`).
  * Use `--all` to fetch all columns.
* Add `snow spcs service metrics` command to fetch service metrics:
  * Supports filtering by service name, container name, instance ID, and time intervals (`--since`, `--until`).
  * Use `--all` to fetch all columns.
* Added support for glob pattern (except `**`) in artifact paths in snowflake.yml for Streamlit.
* Added support for glob pattern (except `**`) in artifact paths in snowflake.yml for Snowpark, requires ENABLE_SNOWPARK_GLOB_SUPPORT feature flag.
* Added command `snow app release-directive add-accounts` and `snow app release-directive remove-accounts`
* Added `snow app release-channel set-accounts` command to set accounts for release channel.
* Added support for Mac Os x86_64 architecture.
* Added `--force-replace` flag in `snow snowpark deploy` command to replace entities even if no changes are detected.
* Changes to notebook commands:
  * Added `snow notebook deploy` command that allows creating a notebook using local file.
  * Add support for containerized notebooks.
  * Added notebooks to `snow object` commands.

## Fixes and improvements
* Fixed MFA caching issue in binaries.
* Fixed auto-completion issue in binaries.


# v3.3.0

## Backward incompatibility

## Deprecations

* Added deprecation message for default Streamlit warehouse

## New additions

* Add support for Release Directives by introducing the following commands:
  * `snow app release-directive list`
  * `snow app release-directive set`
  * `snow app release-directive unset`
* `snow app version create` now returns version, patch, and label in JSON format.
* Add `--from-stage` flag to `snow app version create` to allow version creation from the content of the stage without re-syncing to the stage.
* Add support for release channels:
  * Add support for release channels feature in native app version creation/drop.
  * Add ability to specify release channel when creating application instance from release directive: `snow app run --from-release-directive --channel=<channel>`
  * Add ability to list release channels through `snow app release-channel list` command
  * Add ability to add and remove accounts from release channels through `snow app release-channel add-accounts` and snow app release-channel remove-accounts` commands.
  * Add ability to add/remove versions to/from release channels through `snow app release-channel add-version` and `snow app release-channel remove-version` commands.
* Add publish command to make it easier to manage publishing versions to release channels and updating release directives: `snow app publish`
* Add support for restricting Snowflake user authentication policy to Snowflake CLI-only. For more infotmation, [check the documentation](https://docs.snowflake.com/developer-guide/snowflake-cli/connecting/configure-cli#label-snowcli-limit-access).
* Added a new command: `snow helpers import-snowsql-connections` allowing to import configuration of connections from SnowSQL.

## Fixes and improvements

* Fixed inability to add patches to lowercase quoted versions
* Fixes label being set to blank instead of None when not provided.
* Fixes generate-jwt command to preserve command line connection options.
* Fixed stage path handling for notebook commands.

# v3.2.2

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* Fix "No module named 'pandas'" warning.

# v3.2.1

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* Fixed crashes with older x86_64 Intel CPUs.

# v3.2.0

## Deprecations

* `manifest` field of `application package` entity is now optional. This field does not have any functionality.

## New additions

* Added `--retain-comments` option to `snow sql` command to allow passing comments to Snowflake.
* Added `--replace` and `--if-not-exists` options to `snow object create` command.
* Added support for event sharing, which now can be specified under the `telemetry` section of an application entity. Two fields are supported: `share_mandatory_events` and `optional_shared_events`.
* `snow stage copy` supports `--recursive` flag to copy local files and subdirectories recursively to stage. Including
  glob support.
* `snow helpers v1-to-v2` now converts v1 template references to v2 in Native App artifacts that use the `templates` processor.
* Added `--label` option to `snow app version create` command to allow adding labels to versions and patches.
* Enhanced `snow spcs service logs` command with new parameters for improved log retrieval and monitoring.
  * `--previous-logs`: Retrieve logs from the last terminated container.
  * `--since`: Start log retrieval from a specified UTC timestamp.
  * `--include-timestamps`: Include timestamps in log entries for log streaming.
  * `--follow`: Stream logs in real-time.
  * `--follow-interval`: Set custom polling intervals during log streaming.
* `snow connection add` supports `--no-interactive` flag to skip interactive prompts.

## Fixes and improvements

* `snow --info` callback returns information about `SNOWFLAKE_HOME` variable.
* Removed requirement of existence of any `requirements.txt` file for Python code execution via `snow git execute` command.
  Before the fix the file (even empty) was required to make the execution working.
* Fixed saving of the config file updates when `connections.toml` exists.
  Removed incorrect copying of connections from `connections.toml` to `config.toml`.
* Fixes `snow connection generate-jwt` to work with keys with no passphrase.
* The privilege to create a schema or stage is no longer required to run `snow app version create` if the schema and stage already exist.
* Fix Windows permissions error on files created by CLI when owner is a part of custom group with granted
  default permissions.

# v3.1.0

## Deprecations

* Added deprecation warning in the description of `snow spcs service status` and `snow spcs image-repository list-tags`.
* Completely removed the `snow app init` as it was replaced with `snow init` in Snowflake CLI 3.0

## New additions

* Added `snow connection generate-jwt` command to generate JWT token for Snowflake connection.
* Added `snow spcs service list-instances`, `snow spcs service list-containers` and `snow spcs service list-roles` commands, which support fetching information about all instances/containers/service roles in a service.
* `snow spcs service set` now supports `--eai-name` to update external access integrations for a service.

## Fixes and improvements

* Fixed a bug that would cause the `deploy_root`, `bundle_root`, and `generated_root` directories to be created in the current working directory instead of the project root when invoking commands with the `--project` flag from a different directory.
* Align variables for `snow stage|git execute`. For Python files variables are stripped of leading and trailing quotes.
* `snow spcs service list-images` now displays image tag and digest.
* Fix `snow stage list-files` for paths with directories.
* Fix for list fields in mixins applied twice

# v3.0.2

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* Fixed the handling empty default values for strings by `snow snowpark deploy`.
* Added log error details if the `pip` command fails.
* Fix `snow git execute` support for Python files.

# v3.0.1

## Backward incompatibility

## Deprecations

* Changing package name from `snowflake-cli-labs` to `snowflake-cli`. `Snowflake-cli-labs` will be pointing to the corresponding version of `snowflake-cli`.

## New additions

## Fixes and improvements

# v3.0.0

## Backward incompatibility

* Dropped support for Python below 3.10 version.
* `snow object stage` commands are removed in favour of `snow stage`.
* `snow snowpark init`, `snow streamlit init`, and `snow app init` commands are removed in favor of `snow init` command.
* Removed deprecated flags from `snow snowpark` commands.
* Default Python version for Snowpark functions and procedures was bumped to 3.10 from 3.8.
* Snowpark commands
  * `snow snowpark build` creates a .zip file for each specified artifact that is a directory. Non-Anaconda
    dependencies are packaged once as `dependencies.zip`.
  * `snow snowpark deploy` uploads all artifacts created during build step. Dependencies zip is upload once to
     every Snowpark stage specified in project definition.
  * The changes are compatible with V1 projects definition though the result state (file layout) is different.
  * `snow snowpark package` commands no longer fallback to Anaconda Channel metadata when fetching available packages info fails.
  * Added `snow streamlit execute app-name` command to run Streamlit apps in a Snowflake environment in headless mode.

## Deprecations

* Renamed `private-key-path` flag to `private-key-file`, added `private-key-path` as an alias for backward compatibility.

## New additions

* Added `snow spcs service execute-job` command, which supports creating and executing a job service in the current schema.
* Added `snow app events` command to fetch logs and traces from local and customer app installations.
* Added support for external access (api integrations and secrets) in Streamlit.
* Added support for `<% ... %>` syntax in SQL templating.
* Support multiple Streamlit application in single snowflake.yml project definition file.
* Added `snow helpers v1-to-v2` command to migrate `snowflake.yml` file from V1 to V2.
* Added `--package-entity-id` and `--app-entity-id` options to `snow app` commands to allow targeting specific entities when the `definition_version` in `snowflake.yml` is `2` or higher and it contains multiple `application package` or `application` entities.
* Added templates expansion of arbitrary files for Native Apps through `templates` processor.
* Added `SNOWFLAKE_..._PRIVATE_KEY_RAW` environment variable to pass private key as a raw string.
* Added periodic check for newest version of Snowflake CLI. When new version is available, user will be notified.
* Added support for `imports` in Streamlit definition.
* Add `--host` and `--port` to connection flags.

## Fixes and improvements

* Fixed problem with whitespaces in `snow connection add` command.
* Added check for the correctness of token file and private key paths when addind a connection.
* Fix the typo in spcs service name argument description. It is the identifier of the **service** instead of the **service pool**.
* Fix error handling and improve messaging when no artifacts provided.
* Improved error message for incompatible parameters.
* Fixed SQL error when running `snow app version create` and `snow app version drop` with a version name that isn't a valid Snowflake unquoted identifier
* Duplicated keys in `snowflake.yml` are now detected and reported.
* `snow streamlit deploy` will check for existing streamlit instance before deploying anything.
* Fixed `snow git execute` with `/` in name of the branch.
* `snow app` commands don't enforce ownership of the objects they manage, and rely on RBAC instead.
* `snow app deploy` for package entity now allows operating on application packages created outside the CLI
* Fixes `snow connection add` behavior when `connections.toml` file exists.
* Add more readable error messages in `snow object create` command.

# v2.8.2

## Backward incompatibility

## Deprecations

* Changing package name from `snowflake-cli-labs` to `snowflake-cli`. `Snowflake-cli-labs` will be pointing to the corresponding version of `snowflake-cli`.

## New additions

## Fixes and improvements

# v2.8.1

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* Fixed git execute not working with upper case in directory name.
* Fixed `snow git setup` command behaviour for fully qualified repository names.
* Fixed `snow git setup` command behaviour in case API integration or secret with default name already exists.
* Fixed `snow snowpark package create` creating empty zip when package name contained capital letters.

# v2.8.0

## Backward incompatibility

## Deprecations

* Added deprecation warning for `native_app.package.scripts` in project definition file.

## New additions

* Added support for project definition file defaults in templates.
* Added support for `native_app.package.post_deploy` scripts in project definition file.
  * These scripts will execute whenever a Native App Package is created or updated.
  * Currently only supports SQL scripts: `post_deploy: [{sql_script: script.sql}]`.

## Fixes and improvements

* Fix return values of `snow snowpark list`, `describe` and `drop` commands.
* Show warnings returned by Snowflake when `snow app run` is successful.

# v2.7.0

## Backward incompatibility

## Deprecations

* `snow snowpark init` and `snow streamlit init` are marked as deprecated. The commands are still functional,
but should be replaced with `snow init`

## New additions

* Added connection option `--token-file-path` allowing passing OAuth token using a file. The function is also
  supported by setting `token_file_path` in connection definition.
* Support for Python remote execution via `snow stage execute` and `snow git execute` similar to existing EXECUTE IMMEDIATE support.
* Added support for autocomplete in `--connection` flag.
* Added `snow init` command, which supports initializing projects with external templates.

## Fixes and improvements

* The `snow app run` command now allows upgrading to unversioned mode from a versioned or release mode application installation
* The `snow app teardown` command now allows dropping a package with versions when the `--force` flag is provided
* The `snow app version create` command now allows operating on application packages created outside the CLI
* Added support for user stages in stage execute and copy commands
* Improved support for quoted identifiers in snowpark commands.
* Updated post_deploy SQL script default database to be the application database
* Handle `NULL` md5 values correctly when returned by stage storage backends
* Regionless host URLs are now supported when generating Snowsight URLs
* `snow app run` and `snow app deploy` now correctly determine modified status for large files uploaded to AWS S3

# v2.6.1

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* `snow object create` message returns meaningful error if connection database is not defined.
* Fixed crashing when save_logs is false and log directory does not exist

# v2.6.0

## Backward incompatibility

## Deprecations

## New additions

* Add `snow object create` command
* Added support for `title` field in Streamlit definition in `snowflake.yml` project file.
* Added `--auto-compress` flag to `snow stage copy` command enabling use of gzip to compress files during upload.
* Added new `native_app.application.post_deploy` section to `snowflake.yml` schema to execute actions after the application has been deployed via `snow app run`.
  * Added the `sql_script` hook type to run SQL scripts with template support.
* Added support for `--env` command line arguments for templating.
  * Available for commands that make use of the project definition file.
  * Format of the argument: `--env key1=value1 --env key2=value2`.
  * Overrides `env` variables values when used in templating.
  * Can be referenced in templating through `ctx.env.<key_name>`.
  * Templating will read env vars in this order of priority (highest priority to lowest priority):
    * vars from `--env` command line argument.
    * vars from shell environment variables.
    * vars from `env` section of project definition file.

## Fixes and improvements

* Passing a directory to `snow app deploy` will now deploy any contained file or subfolder specified in the application's artifact rules
* Fixes markup escaping errors in `snow sql` that may occur when users use unintentionally markup-like escape tags.
* Fixed case where `snow app teardown` could not tear down orphan applications (those that have had their package dropped)
* Fixed case where `snow app teardown` could leave behind orphan applications if they were not created by the Snowflake CLI
* Fixed case where `snow app run` could fail to run an existing application whose package was dropped by prompting to drop and recreate the application
* Improve terminal output sanitization to avoid ASCII escape codes.
* The `snow sql` command will show query text before executing it.
* Improved stage diff output in `snow app` commands
* Hid the diff from `snow app validate` output since it was redundant
* Added log into the file with loaded external plugins
* Warn users if they attempt to use templating with project definition version 1
* Improved output and format of Pydantic validation errors
* Improved support for quoted identifiers in streamlit commands.
* The `snow app run` command will no longer override debug mode during an application upgrade unless explicitly set in `snowflake.yml`

# v2.5.0

## Backward incompatibility

## Deprecations

## New additions

* Added `snow app bundle` command that prepares a local folder in the project directory with artifacts to be uploaded to a stage as part of creating a Snowflake Native App.
  * Snowflake Native App projects can now optionally generate CREATE FUNCTION or CREATE PROCEDURE declarations in setup scripts from Snowpark python code that includes decorators (e.g. @sproc, @udf).
* Added `snow app validate` command that validates the setup script SQL used to create a Snowflake Native App for syntax validity, invalid object references, and best practices
  * Added new `native_app.scratch_stage` field to `snowflake.yml` schema to allow customizing the stage that the CLI uses to run the validation
* Changed `snow app deploy` and `snow app run` to trigger validation of the uploaded setup script SQL and block uploads on validation failure, pass `--no-validate` to disable
* Changed `snow app version create --patch` to require an integer patch number, aligning with what Snowflake expects
* Added `snow notebook` commands:
  * `snow notebook execute` enabling head-less execution of a notebook.
  * `snow notebook create` proving an option to create a Snowflake Notebook from a file on stage.
* Added templating support for project definition file.
  * Template variables can now be used anywhere in the project definition file.
* Added `--default` flag to `snow connection add` commands allowing users to mark the new connection as default.

## Fixes and improvements

* Fixed error handling for malformatted `config.toml`
* Fixed ZIP packaging of Snowpark project dependencies containing implicit namespace packages like `snowflake`.
* Deploying function/procedure with `--replace` flag now copies all grants
* Fixed MFA caching
* Fixed `DeprerationWarning`/`SyntaxWarning` due to invalid escape sequences
* Improved error message in `snow spcs image-registry login` when docker is not installed.
* Improved detection of conflicts between artifact rules for native application projects
* Fixed URL generation for applications, streamlits, and notebooks that use a quoted identifier with spaces.

# v2.4.0

## Backward incompatibility

## Deprecations

## New additions

* Added the `--cascade` option to `snow app teardown` command that drops all application objects owned by the application.
* Add external access integration to snow object commands
* Add aliases for `snow object list/describe/drop` commands under:
  * `snow stage` for stages
  * `snow git` for git repository stages
  * `snow streamlit` for streamlit apps
  * `snow snowpark` for procedures and functions
  * `snow spcs compute-pool` for compute pools
  * `snow spcs image-repository` for image repositories
  * `snow spcs service` for services
* `snow sql` works now with `snowflake.yml` file. The variables defined in the new `env` section
       of `snowflake.yml` will be used to expand templates.
* `snow sql` support executing multiple files. Users can use `-f/--file` flag more than once to execute queries
      from many files.
* `snow git execute` and `snow stage execute` support passing input variables for SQL execution.
* Added `snow cortex` commands:
  * `complete` - Given a prompt, the command generates a response using your choice of language model.
      In the simplest use case, the prompt is a single string. You may also provide a JSON file with conversation history including multiple prompts and responses for interactive chat-style usage.
  * `extract-answer` - Extracts an answer to a given question from a text document.
      The document may be a plain-English document or a string representation of a semi-structured (JSON) data object.
  * `sentiment` - Returns sentiment as a score between -1 to 1 (with -1 being the most negative and 1 the most positive, with values around 0 neutral) for the given English-language input text.
  * `summarize` - Summarizes the given English-language input text.
  * `translate` - Translates text from the indicated or detected source language to a target language.
  * `search`    - for integration with Cortex Search Service
* When invoked without command help is displayed by default with list of available commands.
* Add tab-completion for `snow` command.

## Fixes and improvements

* Improved support for quoted identifiers.
* Fixed creating patches with `snow app version create` when there are 2 or more existing patches on a version
* Using `--format=json` adds trailing new line to avoid `%` being added by some terminals to signal no new line at the end of output.
* Fixed `--interactive` flag to be enabled by default in interactive environments and added the `--no-interactive` flag to be able to turn off prompting.

# v2.3.1

## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements

* Fixed bugs in source artifact mapping logic for native applications

# v2.3.0

## Backward incompatibility

## Deprecations

## New additions

* New `snow sql` functionality:
  * `-D/--variable` allows variable substitutions in a SQL input (client-side query templating)
* New `snow app deploy` functionality:
  * Passing files and directories as arguments syncs these only: `snow app deploy some-file some-dir`.
  * `--recursive` syncs all files and subdirectories recursively.
  * `--prune` deletes specified files from the stage if they don't exist locally.

## Fixes and improvements

* More human-friendly errors in case of corrupted `config.toml` file.
* Fixed a bug in `snow app` that caused files to be re-uploaded unnecessarily.
* Optimize snowpark dependency search to lower the size of .zip artifacts and
  the number of anaconda dependencies for snowpark projects.
* Added support for fully qualified stage names in stage and git execute commands.
* Fixed a bug where `snow app run` was not upgrading the application when the local state and remote stage are identical (for example immediately after `snow app deploy`).
* Fixed handling of stage path separators on Windows
* Change to `external_access_integrations` in `snowflake.yml` now also triggers function replace
* The `--info` callback returns info about configured feature flags.

# v2.2.0

## Backward incompatibility

## Deprecations

* `snow snowpark package lookup` no longer performs check against PyPi. Using `--pypi-download` or `--yes`
  has no effect and will cause a warning. In this way the command has single responsibility - check if package is
  available in Snowflake Anaconda channel.
* `snow snowpark package create`:
  * `--pypi-download` or `--yes` flags are deprecated, have no effect and will cause a warning.
    `create` command always checks against PyPi.
  * `--allow-native-libraries` is deprecated in favour of boolean flag `--allow-shared-libraries`.
    Using old flag will cause a warning.
* `snow snowpark build`:
  * `--pypi-download` flag is deprecated, have no effect and will cause a warning. Build command always check against PyPi.
  * `--check-anaconda-for-pypi-depts` is deprecated and using it will cause warning, the functionality is replaced by `--ignore-anaconda`
  * `--package-native-libraries` is deprecated and using it will cause warning, the functionality is replaced by `--allow-shared-libraries`
* `snow object stage` commands are deprecated and using them will cause a warning.
   Functionality is replaced by `snow stage` commands.

## New additions

* Added support for fully qualified name (`database.schema.name`) in `name` parameter in streamlit project definition
* Added support for fully qualified image repository names in `spcs image-repository` commands.
* Added `--if-not-exists` option to `create` commands for `service`, and `compute-pool`. Added `--replace` and `--if-not-exists` options for `image-repository create`.
* Added support for python connector diagnostic report.
* Added `snow app deploy` command that creates an application package and syncs the local changes to the stage without creating or updating the application.
* Added `is_default` column to `snow connection list` output to highlight default connection.
* `snow snowpark package create`:
  * new `--ignore-anaconda` flag disables package lookup in Snowflake Anaconda channel.
    All dependencies will be downloaded from PyPi.
  * new `--skip-version-check` skips comparing versions of dependencies between requirements and Anaconda.
  * new `--index-url` flag sets up Base URL of the Python Package Index to use for package lookup.
* `snow snowpark build`:
  * new `--skip-version-check` skips comparing versions of dependencies between requirements and Anaconda.
  * new `--index-url` flag sets up Base URL of the Python Package Index to use for package lookup.
* Added `--recursive` flag for copy from stage, it will reproduce the directory structure locally.
* Added support for snowgit. New commands:
  * `snow git setup` - wizard setting up a git repository stage and creating all necessary objects
  * `snow git fetch` - fetches latest changes from the origin repository into Snowflake repository
  * `snow git list-branches` - lists all branches in the repository
  * `snow git list-tags` - lists all tags in the repository
  * `snow git list-files` - lists all files on provided branch/tag/commit
  * `snow git copy` - copies files from provided branch/tag/commit into stage or local directory
  * `snow git execute` - execute immediate files from repository
* Added command for execute immediate `snow object stage execute`
* Fetching available packages list from Snowflake instead of directly from Anaconda with fallback to the old method (for backward compatibility).
  As the new method requires a connection to Snowflake, it adds connection options to the following commands:
  * `snow snowpark build`
  * `snow snowpark package lookup`
  * `snow snowpark package create`

## Fixes and improvements

* Adding `--image-name` option for image name argument in `spcs image-repository list-tags` for consistency with other commands.
* Fixed errors during `spcs image-registry login` not being formatted correctly.
* Project definition no longer accept extra fields. Any extra field will cause an error.
* Changing imports in function/procedure section in `snowflake.yml` will cause the definition update on replace
* Adding `--pattern` flag to `stage list-files` command for filtering out results with regex.
* Fixed snowpark build paths for builds with --project option (fixed empty zip issue).
* More clear error messages in `snow snowpark build` command
* Adding support for any source supported by `pip` in `snow snowpark`.
* Fixed version parsing for packages lookup on Snowflake Anaconda Channel
* Fix handling database/schema/role identifiers containing dashes
* Fix schema override bug in `snow connection test`
* Hidden incorrectly working config permissions warning on Windows
* Make errors from `snow connection test` more meaningful when role, warehouse or database does not exist.

# v2.1.2

## Fixes and improvements

* Add `pip` as Snowflake-cli dependency
* Optimize `connection test` command
* Fix venv creation issues in `snowpark package create` and `snowpark build` command

# v2.1.1

## Fixes and improvements

* Improved security of printing connection details in `snow connection list`.
* Improved parsing of non-quoted scalar values within square brackets in `manifest.yml` in Native App projects.

# v2.1.0

## Backward incompatibility

## New additions

* Added ability to specify scope of the `object list` command with the `--in <scope_type> <scope_name>` option.
* Introduced `snowflake.cli.api.console.cli_console` object with helper methods for intermediate output.
* Added new `--mfa-passcode` flag to support MFA.
* Added possibility to specify `database` and `schema` in snowflake.yml for snowpark objects. Also `name` can specify a fully qualify name.
* New commands for `spcs`
  * Added `image-registry url` command to get the URL for your account image registry.
  * Added `image-registry login` command to fetch authentication token and log in to image registry in one command.
  * Added `image-repository url <repo_name>` command to get the URL for specified image repository.
  * Added `create` command for `image-repository`.
  * Added `status`, `set (property)`, `unset (property)`, `suspend` and `resume` commands for `compute-pool`.
  * Added `set (property)`, `unset (property)`,`upgrade` and `list-endpoints` commands for `service`.
* You can now use github repo link in `snow snowpark package create` to prepare your code for upload
* Added `allow-native-libraries` option to `snow snowpark package create` command
* Added alias `--install-from-pip` for `-y` option in `snow snowpark package create` command
* Connections parameters are also supported by generic environment variables:
  * `SNOWFLAKE_ACCOUNT`
  * `SNOWFLAKE_USER`
  * `SNOWFLAKE_PASSWORD`
  * `SNOWFLAKE_DATABASE`
  * `SNOWFLAKE_SCHEMA`
  * `SNOWFLAKE_ROLE`
  * `SNOWFLAKE_WAREHOUSE`
  * `SNOWFLAKE_MFA_PASSCODE`
* Introduced `--pypi-download` flag for `snow snowpark package` commands to replace `-y` and `--yes`

  The `SNOWFLAKE_CONNECTION_<NAME>_<KEY>` variable takes precedence before the generic flag. For example if
  `SNOWFLAKE_PASSWORD` and `SNOWFLAKE_CONNECTIONS_FOO_PASSWORD` are present and user tries to use connection
  "foo" then the later variable will be used.
* Testing connection using `snow connection test` validates also access to database, schema, role and warehouse
  specified in the connection details.
* Added `snow connection set-default` command for changing default connection.

## Fixes and improvements

* Restricted permissions of automatically created files
* Fixed bug where `spcs service create` would not throw error if service with specified name already exists.
* Improved package lookup, to avoid unnecessary uploads
* Logging into the file by default (INFO level)
* Added validation that service, compute pool, and image repository names are unqualified identifiers.
* `spcs service` commands now accept qualified names.
* Updated help messages for `spcs` commands.

# v2.0.0

## Backward incompatibility

* Introduced `snow object` group with `list`, `describe` and `drop` commands which replaces corresponding
  functionalities of procedure/function/streamlit specific commands.
* `snow stage` is now `snow object stage`
* `snow stage get` and `snow stage put` are replaced by `snow object stage copy [FROM] [TO]`
* `snow warehouse status` is now `snow object list warehouse`
* `snow connection test` now outputs all connection details (except for the password), along with connection status
* `snow sql` requires explicit `-i` flag to read input from stdin: `cat my.sql | snow sql -i`
* Switched to Python Connector default connection <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection>
  * Default connection name changed from `dev` to `default`
  * Environment variable for default connection name changed from `SNOWFLAKE_OPTIONS_DEFAULT_CONNECTION` to `SNOWFLAKE_DEFAULT_CONNECTION_NAME`

* Snowpark changes
  * Removed `procedure` and `function` subgroups.
  * Removed `snow snowpark function package` and `snow snowpark procedure package` in favour of `snow snowpark build`.
  * Removed `snow snowpark function create` and `snow snowpark function update`. Functions can be deployed using `snow snowpark deploy`.
  * Removed `snow snowpark procedure create` and `snow snowpark procedure update`. Procedures can be deployed using `snow snowpark deploy`.
  * Procedures and functions use single zip artifact for all functions and procedures in project.
  * Changed path to coverage reports on stage, previously created procedures with coverage will not work, have to be recreated.
  * Previously created procedures or functions won't work with `deploy` command due to change in stage path of artifact. Previous code will remain under old path on stage.
  * Package commands are now under `snow snowpark package`.
  * Coverage commands were removed. To measure coverage of your procedures or functions use coverage locally.

* Snowpark Containers services commands
  * `cp` alias for `compute-pool` commands was removed.
  * `services` commands were renamed to `service`
  * `registry` commands were renamed to `image-registry`
  * `compute-pool`, `service`, and `image-registry` commands were moved from `snowpark` group to a new `spcs` group.
  * `snow spcs compute-pool create` and `snow spcs service create` have been updated with new options to match SQL interface.
  * Added new `image-repository` command group under `spcs`. Moved `list-images` and `list-tags` from `image-registry` to `image-repository`.
  * Removed `snow snowpark jobs` command.
  * `list-images` and `list-tags` now outputs image names with a slash at the beginning (e.g. /db/schema/repo/image). Image name input to `list-tags` requires new format.
  * `snow spcs compute-pool stop` has been removed in favor of `snow spcs compute-pool stop-all`.

* Streamlit changes
  * `snow streamlit deploy` is requiring `snowflake.yml` project file with a Streamlit definition.
  * `snow streamlit describe` is now `snow object describe streamlit`
  * `snow streamlit list` is now `snow object list streamlit`
  * `snow streamlit drop` is now `snow object drop streamlit`

## New additions

* Added `snow streamlit get-url [NAME]` command that returns url to a Streamlit app.
* `--temporary-connection` flag, that allows you to connect, without anything declared in config file
* Added project definition for Streamlit
* Added `snow streamlit get-url [NAME]` command that returns url to a Streamlit app.
* Added project definition for Snowpark procedures and functions.
  * The `snowflake.yml` file is required to deploy functions or procedures.
  * Introduced new `deploy` command for project with procedures and functions.
  * Introduced new `build` command for project with procedure and functions
* Added support for external access integration for functions and procedures
* Added support for runtime version in snowpark procedures ad functions.
* You can include previously uploaded packages in your functions, by listing them under `imports` in `snowflake.yml`
* Added more options to `snow connection add` - now you can also specify authenticator and path to private key
* Added support for native applications by introducing new commands.
  * `snow app init` command that creates a new Native App project from a git repository as a template.
  * `snow app version create` command that creates or upgrades an application package and creates a version or patch for that package.
  * `snow app version drop` command that drops a version associated with an application package.
  * `snow app version list` command that lists all versions associated with an application package.
  * `snow app run` command that creates or upgrades an application in development mode or through release directives.
  * `snow app open` command that opens the application inside of your browser on Snowsight, once it has been installed in your account.
  * `snow app teardown` command that attempts to drop both the application and package as defined in the project definition file.
* Snowpark: add `default` field to procedure and function arguments definition in `snowflake.yml` to support [named and optional
  arguments](https://docs.snowflake.com/en/developer-guide/udf/udf-calling-sql#calling-a-udf-that-has-optional-arguments)

## Fixes and improvements

* Allow the use of quoted identifiers in stages

# v1.2.5

## Fixes and improvements

* Import git module only when is needed

# v1.2.4

## Fixes and improvements

* Fixed look up for all folders in downloaded package.

# v1.2.3

## Fixes and improvements

* Removed hardcoded values of instance families for `snow snowpark pool create` command.

# v1.2.2

## Fixes and improvements

* Fixed parsing of commands and arguments lists in specifications of snowpark services and jobs

# v1.2.1

## Fixes and improvements

* Fix homebrew installation

# v1.2.0

## Backward incompatibility

* Removed `snow streamlit create` command. Streamlit can be deployed using `snow streamlit deploy`
* Removed short option names in compute pool commands:
  * `-n` for `--name`, name of compute pool
  * `-d` for `--num`, number of pool's instances
  * `-f` for `--family`, instance family
* Renamed long options in Snowpark services commands:
  * `--compute_pool` is now `--compute-pool`
  * `--num_instances` is now `--num-instances`
  * `--container_name` is now `--container-name`

## New additions

* `snow streamlit init` command that creates a new streamlit project.
* `snow streamlit deploy` support pages and environment.yml files.
* Support for private key authentication

## Fixes and improvements

* Adjust streamlit commands to PuPr syntax
* Fix URL to streamlit dashboards

# v1.1.1

## Backward incompatibility

* Removed short version `-p` of `--password` option.

## New additions

* Added commands:
  * `snow snowpark registry list-images`
  * `snow snowpark registry list-tags`

## Fixes and improvements

* Too long texts in table cells are now wrapped instead of cropped
* Split global options into separate section in `help`
* Avoiding unnecessary replace in function/procedure update
* Added global options to all commands
* Updated help messages
* Fixed problem with Windows shortened paths
* If only one connection is configured, will be used as default
* Fixed registry token connection issues
* Fixes in commands belonging to `snow snowpark compute-pool` and `snow snowpark services` groups
* Removed duplicated short option names in a few commands by:
  * Removing `-p` short option for `--password` option for all commands (backward incompatibility affecting all the commands using a connection) (it was conflicting with various options in a few commands)
  * Removing `-a` short option for `--replace-always` in `snow snowpark function update` command (it was conflicting with short version of `--check-anaconda-for-pypi-deps`)
  * Removing `-c` short option for `--compute-pool` in `snow snowpark jobs create` (it was conflicting with short version of global `--connection` option)
  * Removing `-c` short option for `--container-name` in `snow snowpark jobs logs` (it was conflicting with short version of global `--connection` option)
* Fixed parsing of specs yaml in `snow snowpark services create` command
