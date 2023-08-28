## Introduction

This is the default project template for a Snowflake Native Apps project. It contains minimal code meant to help you set up your first application instance in your account quickly.

### Project Structure
| File Name | Purpose |
| --------- | ------- |
| ./README.md | The current file you are looking at, meant to guide you through a native apps project. |
| app/setup_script.sql | Contains SQL statements that are run when an account installs or upgrades an application. |
| app/manifest.yml | Defines properties required by the application package. Find more details at the [Manifest Documentation.](https://docs.snowflake.com/en/developer-guide/native-apps/creating-manifest)
| app/README.md | Exposed to the account installing the application with details on purpose and how to use the application. |
| .snowflake/config.yml | Used by the snowCLI tool to upload your code to a snowflake stage and create an application package from it with all relevant permissions and grants. |

### Using the local.yml file
Your `./snowflake` directory already comes with a `config.yml` file and a `local.yml` file.

The `config.yml` is meant to be shared between different developers of the same native app project in your version control.

However, an individual developer can choose to customize the behavior of the snowCLI by providing local overrides to the snowflake account, such as a new dummy role to test out your own application package.This is where you can use `local.yml`, which is not a version-controlled file.

Edit the file with relevant values to the fields, defaults are described below.
```
package:
  role: <your_app_pkg_owner_role, accountadmin by default>
  name: <name_of_app_pkg, project_name_pkg by default>

application:
  role: <your_app_owner_role, accountadmin by default>
  name: <name_of_app, project_name_app by default>
  debug: <true|false, true by default>

```
