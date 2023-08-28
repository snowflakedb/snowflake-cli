## Introduction

This is the default project template for a Snowflake Native Apps project. It contains minimal code meant to help you set up your first application instance in your account quickly.

### Project Structure
| File Name | Purpose |
| --------- | ------- |
| README.md | The current file you are looking at, meant to guide you through a native apps project. |
| app/setup_script.sql | Contains SQL statements that are run when an account installs or upgrades an application. |
| app/manifest.yml | Defines properties required by the application package. Find more details at the [Manifest Documentation.](https://docs.snowflake.com/en/developer-guide/native-apps/creating-manifest)
| app/README.md | Exposed to the account installing the application with details on purpose and how to use the application. |
| snowflake.yml | Used by the snowCLI tool to discover your project's code and interact with snowflake with all relevant permissions and grants. |

### Adding a snowflake.local.yml file
Though your project directory already comes with a `snowflake.yml` file,an individual developer can choose to customize the behavior of the snowCLI by providing local overrides to `snowflake.yml`, such as a new role to test out your own application package.This is where you can use `snowflake.local.yml`, which is not a version-controlled file.

Create a `snowflake.local.yml` file with relevant values to the fields, defaults are described below.
```
package:
  role: <your_app_pkg_owner_role, resolved connection* role by default>
  name: <name_of_app_pkg, project_name_pkg_$USER by default>

application:
  role: <your_app_owner_role, accountadmin by default>
  name: <name_of_app, project_name_$USER by default>
  debug: <true|false, true by default>
  warehouse: <your_app_warehouse, resolved connection* warehouse by default>

```
resolved connection* - If snowCLI was installed correctly, there should be a global [config.toml](https://docs.snowflake.com/LIMITEDACCESS/snowcli/connecting/connect#how-to-add-snowflake-credentials-using-a-configuration-file) file where you specify a connection, and a user for that connection. The connection role is derived from this user, or via a role override within the config.toml file. Similarly, a connection warehouse is also derived from a warehouse specified as part of a connection.
