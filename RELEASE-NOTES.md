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

## New additions
* `--temporary-connection` flag, that allows you to connect, without anything declared in config file
* `snow streamlit init` command that creates a new streamlit project.

## Fixes and improvements
* Adjust streamlit commands to PuPr syntax
* Too long texts in table cells are now wrapped instead of cropped
* Split global options into separate section in `help`
* Avoiding unnecessary replace in function/procedure update
* Added global options to all commands
* Updated help messages
* Fixed problem with Windows shortened paths
* If only one connection is configured, will be used as default
