# v2.0.0

## Backward incompatibility
* Introduced `snow object` group with `show`, `describe` and `drop` commands which replaces corresponding
  functionalities of procedure/function/streamlit specific commands.
* `snow stage` is now `snow object stage`
* `snow stage get` and `snow stage put` are replaced by `snow object stage copy [FROM] [TO]`
* `snow warehouse status` is now `snow object show warehouse`
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
  * `compute-pool` commands and its alias `cp` were renamed to `pool` commands.
  * `services` commands were renamed to `service`
  * `pool`, `service`, and `image-registry` commands were moved from `snowpark` group to a new `spcs` group (`registry` was renamed to `image-registry`).
  * `snow spcs pool create` and `snow spcs service create` have been updated with new options to match SQL interface
  * Added new `image-repository` command group under `spcs`. Moved `list-images` and `list-tags` from `registry` to `image-repository`.
  * Removed `snow snowpark jobs` command.

* Streamlit changes
  * `snow streamlit deploy` is requiring `snowflake.yml` project file with a Streamlit definition.
  * `snow streamlit describe` is now `snow object describe streamlit`
  * `snow streamlit list` is now `snow object show streamlit`
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
* Removed `snow streamlit create` command. Streamlit can be deployd using `snow streamlit deploy`
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
