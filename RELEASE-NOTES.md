# v2.0.0

## Backward incompatibility
* Snowpark changes
  * Removed `procedure` and `function` subgroups.
  * Removed `snow snowpark function create` and `snow snowpark function update`. Procedures can be deployed using `snow snowpark deploy`.
  * Removed `snow snowpark procedure create` and `snow snowpark procedure update`. Procedures can be deployed using `snow snowpark deploy`.
  * Procedures and functions use single zip artifact for all functions and procedures.
  * Changed path to coverage reports on stage, previously created procedures with coverage will not work, have to be recreated.
  * Previously created procedures or functions won't work with `deploy` command due to change in stage path of artefact. Previous code will remain under old path on stage.
* Snowpark Containers services commands
  * `compute-pool` commands and its alias `cp` were renamed to `pool` commands.
  * `jobs` commands were renamed to `job`.
  * `services` commands were renamed to `service`
  * `pool`, `job` and `service` commands were moved from `snowpark` group to a new `containers` group.
* `snow snowpark registry` was replaced with `snow registry` command.
* `snow connection test` now outputs all connection details (except for the password), along with connection status
* Streamlit changes
  * `snow streamlit deploy` is requiring `snowflake.yml` project file with a Streamlit definition.
  * `snow streamlit describe` is now `snow object describe streamlit`
  * `snow streamlit list` is now `snow object show streamlit`
  * `snow streamlit drop` is now `snow object drop streamlit`
* `init` commands for functions and procedures create new project in new directory instead of using current working directory.
* Moved `snow stage` from top-level to `snow object` subgroup
* `snow warehouse status` is now `snow object show warehouse`
* Introduced `snow object` group with `show`, `describe` and `drop` commands for: compute pools,
databases, tables, warehouses, functions, procedures, roles, schemas, services, jobs and streamlits
* Removed describe, list/show and drop commands from `snow snowpark` and `snow containers` subgroups


## New additions
* Added `snow streamlit get-url [NAME]` command that returns url to a Streamlit app.
* `--temporary-connection` flag, that allows you to connect, without anything declared in config file
* Added project definition for Streamlit
* Added project definition for Snowpark procedures and functions.
  * The `snowflake.yml` file is required to deploy functions or procedures.
  * Introduced new `deploy` command for project with procedures and functions.
  * Introduced new `build` command for project with procedure and functions

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
