# v2.0.0

## Backward incompatibility
* In `snowpark function` command:
  * Combined options `--function` and `--input-parameters` to `identifier` argument
  * Changed name of option from `--return-type` to `returns`
* In `snowpark procedure` command:
  * Combined options `--procedure` and `--input-parameters` to `identifier` argument
  * Changed name of option from `--return-type` to `--returns`
* In `snowpark procedure coverage` command:
  * Combined options `--name` and `--input-parameters` to `identifier` argument
* Changed path to coverage reports on stage, previously created procedures with coverage will not work, have to be recreated
* Snowpark command `compute-pool` and its alias `cp` were replaced by `pool` command.
* `snow snowpark registry` was replaced with `snow registry` command.

## New additions

## Fixes and improvements
* Split global options into separate section in `help`
* Avoiding unnecessary replace in function/procedure update
* Added global options to all commands
* Updated help messages
* Fixed problem with Windows shortened paths
* If only one connection is configured, will be used as default
