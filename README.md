# Snowflake Developer CLI

**Note**: Snowflake CLI is in Private Preview (PrPr). You must register for the PrPr to use Snowflake CLI by filling out the
[Snowflake CLI  - PrPr Intake Form](https://forms.gle/HZNhPNbzn7oExjFu8). Also, if you want to access Snowflake Container
Services through Snowflake CLI, you must register for its PrPr. For more information, you can contact a
Snowflake sales representative.

For complete installation and usage instructions, refer to the
[Snowflake CLI Guide](https://docs.snowflake.com/LIMITEDACCESS/snowcli/snowcli-guide).

## Overview

Snowflake CLI is a command line interface for working with Snowflake. It lets you create, manage, update, and view apps running in Snowflake.

This is an open source project and contributions are welcome (though the project is maintained on a best-effort basis).

We plan to incorporate some patterns and features of this CLI into the Snowflake CLI (SnowSQL) in the future. We hope this project starts a conversation about what a delightful developer experience could look like, and we'd love your help in shaping that with us!

## Benefits of Snowflake CLI

Snowflake CLI lets you locally run and debug Snowflake apps, and has the following benefits:

- Search, create, and upload python packages that may not be yet supported in Anaconda.
- Has support for Snowpark Python **user defined functions** and **stored procedures**, **warehouses**, and **Streamlit** apps.
- Define packages using `requirements.txt`, with dependencies automatically added via integration with Anaconda at deploy time.
- Use packages in `requirements.txt` that aren't yet in Anaconda and have them manually included in the application package deployed to Snowflake (only works with packages that don't rely on native libraries).
- Update existing applications with code and dependencies automatically altered as needed.
- Deployment artifacts are automatically managed and uploaded to Snowflake stages.

## Limitations of Snowflake CLI

Snowflake CLI has the following limitation:

- To run Streamlit in Snowflake using Snowflake CLI, your Snowflake account must have access to the Streamlit private preview.

## Install Snowflake CLI

### Install with Homebrew (Mac only)

Requires [Homebrew](https://brew.sh/).

```bash
brew tap Snowflake-Labs/snowcli
brew install snowcli
snow --help
```

### Install with pip (PyPi)

Requires Python >= 3.8

```bash
pip install snowflake-cli-labs
snow --help
```

### Install from source

Requires Python >= 3.8 and git

```bash
git clone https://github.com/snowflake-labs/snowcli
cd snowcli
# you can also do the below in an active virtual environment:
# python -m venv .venv
# source .venv/bin/activate
hatch build && pip install .
snow --version
```

You should now be able to run `snow` and get the CLI message.

## Get started using Snowflake CLI

Use Snowflake CLI to build a function or stored procedure, or create a streamlit if you have access to the Streamlit in Snowflake private preview.

### Prerequisites

You must add your credentials to connect to Snowflake before you can use Snowflake CLI. You can add your Snowflake credentials using any of the following methods.

#### Add credentials with Snowflake CLI

To add Snowflake credentials using the Snowflake CLI `connection add` command:

1. Enter the following shell command and supply the connection, account, username, and password when prompted:

   ```
   $ snow connection add
   Name for this connection: <connection-name>
   Snowflake account: <account-name>
   Snowflake username: <username>
   Snowflake password: <password>
   ```

#### Add credentials using a configuration file

Snowflake CLI lets you add connection definitions to a configuration file.
A connection definition refers to a collection of connection parameters.

To add credentials in a configuration file:

1. In your home directory, create a **~/.snowflake** directory:

   ```
   $ mkdir ~/.snowflake
   ```

1. Create a **config.toml** file in that directory:

   ```
   $ cd ~/.snowflake
   $ touch config.toml
   ```

1. In a text editor, open the **config.toml** file for editing, such as the following for the Linux vi editor:

   ```
   $ vi config.toml
   ```

1. Add a new Snowflake connection definition. You must prefix the configuration with **connections**.

   For example, to add a Snowflake connection called **myconnection** with the credentials: account **myaccount**,
   user profile **johndoe**, and password **hunter2**,
   add the following lines to the configuration file:

   ```
   [connections]
   [connections.myconnection]
   account = "myaccount"
   user = "jondoe"
   password = "hunter2"
   ```

1. If desired, you can add more connections, as shown:

   ```
   [connections]
   [connections.myconnection]
   account = "myaccount"
   user = "jondoe"
   password = "hunter2"

   [connections.myconnection-test]
   account = "myaccount"
   user = "jondoe-test"
   password = "hunter2"
   ```

1. Save changes to the file.


#### Use environment variables for Snowflake credentials

If you prefer, you can specify Snowflake credentials in system environment variables, instead of specifying them
in configuration files. You can use environment variables only to replace connection parameters. Environment variables for
the configuration must use the following formats:

- ``SNOWFLAKE_<section-name>_<key>=<value>``
- ``SNOWFLAKE_<section-name>_<option-name>_<key>=<value>``

where:

- ``<section-name>`` is the name of the section in the configuration file.
- ``<option-name>`` is the name of the option in the configuration file.
- ``<key>`` is the name of the key

For example: SNOWFLAKE_CONNECTIONS_MYCONNECTION_ACCOUNT="my-account"


You specify some credentials, such as account and user, in the configuration file while specifying the password in an
environment variables as follows:

1. Define the following environment variables, as appropriate for your operating system:

   ```
   [connections]
   [connections.myconnection]
   account = "myaccount"
   user = "jdoe"
   ```

1. Create a system environment variable for the password using the appropriate naming convention:

   ```
   SNOWFLAKE_CONNECTIONS_MYCONNECTION_PASSWORD=pass1234
   ```

You can also override a value in the configuration file using a system environment variable. Assume the **config.toml**
file contains the following:

```
[connections]
[connections.myconnection]
account = "myaccount"
user = "jdoe"
password = "xyz2000"
```

You can supply a different password for that connection by creating the following environment variables:

```
SNOWFLAKE_CONNECTIONS_MYCONNECTION_PASSWORD=pass1234
```

In these two examples, Snowflake CLI uses the password "pass1234".

## Get involved

Have a feature idea? Running into a bug? Want to contribute? We'd love to hear from you!
Please open or review issues, open pull requests, or reach out to us on developers@snowflake.com
