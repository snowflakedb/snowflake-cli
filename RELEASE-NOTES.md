# Unreleased version
## Backward incompatibility

## Deprecations

## New additions

## Fixes and improvements


# v2.3.0

## Backward incompatibility

## Deprecations

## New additions
* `snow sql` command supports now client-side templating of queries
* New `snow sql` functionality:
  * `-D/--data` allows variable substitutions in a SQL input (client-side query templating)
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
* Switched to Python Connector default connection https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection
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
