# v2.0.0

## Backward incompatibility
* In `snowpark function` command:
  * Combined options `--function` and `--input-parameters` to `identifier` argument.
  * Changed name of option from `--return-type` to `returns`.
* In `snowpark procedure` command:
  * Combined options `--procedure` and `--input-parameters` to `identifier` argument.
  * Changed name of option from `--return-type` to `--returns`.
* In `snowpark procedure coverage` command:
  * Combined options `--name` and `--input-parameters` to `identifier` argument.
* Changed path to coverage reports on stage, previously created procedures with coverage will not work, have to be recreated.
* Update function or procedure will upload function/procedure code to new path on stage. Previous code will remain under old path on stage.
* Snowpark command `compute-pool` and its alias `cp` were replaced by `pool` command.
* `snow snowpark registry` was replaced with `snow registry` command.
* `snow connection test` now outputs all connection details (except for the password), along with connection status
* Removed `snow snowpark function create` and `snow snowpark function update`. Procedures can be deployed using `snow snowpark function deploy`.
* Removed `snow snowpark procedure create` and `snow snowpark procedure update`. Procedures can be deployed using `snow snowpark procedure deploy`.
* From `snow streamlit deploy` moved following parameters to `snowflake.yml`:
  * Argument:
    * `streamlit-name`
  * Flags:
    * `--file`
    * `--stage`
    * `--env-file`
    * `--pages-dir`
* `init` commands for functions and procedures create new project in new directory instead of using current working directory.

## New additions
* `--temporary-connection` flag, that allows you to connect, without anything declared in config file
* Added project definition for Streamlit

## Fixes and improvements
* Resolved `-a` option conflict in `snow snowpark procedure update` command by removing short version of `--replace-always` option (it was conflicting with short version of `--check-anaconda-for-pypi-deps`).


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
