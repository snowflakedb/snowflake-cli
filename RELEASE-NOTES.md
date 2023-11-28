# v2.0.0

## Backward incompatibility
* Introduced `snow object` group with `show`, `describe` and `drop` commands which replaces corresponding
  functionalities of procedure/function/streamlit specific commands.
* `snow stage` is now `snow object stage`
* `snow stage get` and `snow stage put` are replaced by `snow object stage copy [FROM] [TO]`
* `snow warehouse status` is now `snow object show warehouse`
* `snow connection test` now outputs all connection details (except for the password), along with connection status

* Snowpark changes
  * Removed `procedure` and `function` subgroups.
  * Removed `snow snowpark function package` and `snow snowpark procedure package` in favour of `snow snowpark build`.
  * Removed `snow snowpark function create` and `snow snowpark function update`. Functions can be deployed using `snow snowpark deploy`.
  * Removed `snow snowpark procedure create` and `snow snowpark procedure update`. Procedures can be deployed using `snow snowpark deploy`.
  * Procedures and functions use single zip artifact for all functions and procedures in project.
  * Changed path to coverage reports on stage, previously created procedures with coverage will not work, have to be recreated.
  * Previously created procedures or functions won't work with `deploy` command due to change in stage path of artefact. Previous code will remain under old path on stage.
  * Coverage commands are now under `snow snowpark coverage`.
  * Package commands are now under `snow snowpark package`.

* Snowpark Containers services commands
  * `compute-pool` commands and its alias `cp` were renamed to `pool` commands.
  * `jobs` commands were renamed to `job`.
  * `services` commands were renamed to `service`
  * `pool`, `job` and `service` commands were moved from `snowpark` group to a new `containers` group.
  * `snow snowpark registry` was replaced with `snow registry` command.

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

## Fixes and improvements
* Allow the use of quoted identifiers in stages
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
