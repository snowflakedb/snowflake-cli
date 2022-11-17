# Snowflake Developer CLI

⚠️ This is an early concept CLI for working with Snowflake. It is not officially supported by Snowflake, and is not intended for production use. ⚠️

## Overview

The SnowCLI is a command line interface for working with Snowflake. It allows users to create, manage, update, and view apps running in Snowflake. This is an open source project and contributions are welcome (though the project is maintained on a best-effort basis). We do plan to incorporate some of the patterns and features of this CLI into the Snowflake CLI (SnowSQL) in the future, and are hoping this project will help start the conversation on what a delightful developer experience could look like - and would love your help in shaping that with us.

### CLI tour and quickstart
[![SnowCLI overview and quickstart demo](https://i.imgur.com/tqLVPWnm.png)](https://youtu.be/WDuBeAgbTt4)

## Benefits
- Supports local debugging and running of Snowflake apps.
- Define packages using `requirements.txt`, with dependencies automatically added via integration with Anaconda at deploy time.
- Use packages in `requirements.txt` that aren't yet in Anaconda and have them manually included in the application package deployed to Snowflake (only works with packages that don't rely on native libraries).
- Update existing applications with code and dependencies automatically altered as needed.
- Deployment artifacts are automatically managed and uploaded to Snowflake stages.

## Limitations
- Uses the [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql.html) configuration file for authentication. If you do not have the SnowSQL installed you will need to manually create your own config file at `~/.snowsql/config` or `%USERPROFILE%\.snowsql\config` to store connection parameters.
- Has support for Snowpark Python **user defined functions** and **stored procedures**, **warehouses**, and **Streamlit** apps.
- Running Streamlit in Snowflake requires access to the Streamlit private preview.
- Authentication and connections are not cached between calls - so when using authentication `externalbrowser` you will need to authenticate via browser every command ([#19](https://github.com/Snowflake-Labs/snowcli/issues/19)).
- Primarily tested on MacOS and Linux. Windows support is not yet validated.

## Installation

### Install with Homebrew (mac only)

Requires [Homebrew](https://brew.sh/).

```bash
brew tap sfc-gh-jhollan/snowcli
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

Requires Python >= 3.10 and git

```bash
git clone https://github.com/snowflake-labs/snowcli
cd snowcli
# you can also do the below in an active virtual environment:
# python -m venv .venv
# source .venv/bin/activate
pip install -r requirements.txt
hatch build && pip install .
snow --help
```

You should now be able to run `snow` and get the CLI message.

## Getting started

### Adding connection credentials

#### Snow CLI
`snow connection add`

#### Manually
1. Open the SnowSQL configuration file at `~/.snowsql/config` or `%USERPROFILE%\.snowsql\config`.
1. Add a new configuration for your Snowflake connection (be sure to prefix with `connections.`).

For example:
```ini
[connections.connection_name]
accountname = myaccount
username = jondoe
password = hunter2
```

### Building a function
1. Navigate to an empty directory to create your function.
1. Run the command: `snow function init`
    It should populate this directory with the files for a basic function. You can open `app.py` to see the files.
1. Test the code: `python app.py`
    You should see the message: `Hello World!`
1. Package the function: `snow function package`
    This will create an `app.zip` file that has your files in it
1. Login to snowflake: `snow login`
1. Configure your first environment: `snow configure`
1. Create a function: `snow function create`
1. Try running the function: `snow function execute -f 'helloFunction()'
    You should see Snowflake return the message: 'Hello World!'

You can now go modify and edit your `app.py`, `requirements.txt`, or other files and follow a similar flow, or update a function with `snow function update -n myfunction -f app.zip`

### Creating a Streamlit
1. Change to a directory with an existing streamlit app (or create one)
1. Run: `snow login` to select your snowsql config
1. Run: `snow configure` to create an environment and select your database, schema, role, and warehouse (environment name defaults to 'dev')
1. Run: `snow streamlit create <name>` to create a streamlit with a given name (file defaults to streamlit_app.py)
1. Run: `snow streamlit deploy <name> -o` to deploy your app and open it in the browser

### Creating a stored procedure
Follow the same flow as [Building a function](#building-a-function) but replace the `function` command with `procedure`.

## Get involved

Have a feature idea? Running into a bug? Want to contribute? We'd love to hear from you! Please open or review issues, open pull requests, or reach out to us on Twitter / LinkedIn [@jeffhollan](https://twitter.com/jeffhollan) and [@jroes](https://twitter.com/jroes).
