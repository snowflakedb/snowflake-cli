# Snowflake Developer CLI

⚠️ This is a proof of concept CLI built for fun to help me with my inner flow. No SLAs around support or issues. Use at your own risk.

## Overview

This is a work-in-progress CLI for helping in creating apps in Snowflake. It does the following:
- Allow you to initialize a new local directory for your project
- `build` command to help bundle up all application files
- Automatically resolve packages from `requirements.txt`, and check against the Snowflake Anaconda channel
- Option to manually resolve packages that don't exist in Anaconda channel (can not rely on native libaries)
- Deployment artifacts manually managed for you - so you don't have to deal with stage files yourself
- Smart deployment logic that can either update in-place, or replace the function if a change in required packages
- `execute` command to try your function running in Snowflake

## Limitations
- Only been tested on Linux and Mac. Not sure if it works on Windows.
- Basic support for Python functions (describe, execute, package, create) and Streamlits (create, deploy, list)

## Installation

### Pre-requisites
- Python (written using Python 3.10)
- Pip
- Git

### Installation
Navigate to the directory you want to install in. Then run the following:

```bash
git clone https://github.com/snowflake-labs/snowcli
cd snowcli
# you can also do the below in an active virtual environment:
# python -m venv .venv
# source .venv/bin/activate
pip install -r requirements.txt
hatch build && pip install .
```

You should now be able to run `snow` and get the CLI message.

## Getting started

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
1. Create a function: `snow function create -n helloFunction -h 'app.hello' -f app.zip -i '' -r string`
1. Try running the function: `snow function execute -f 'helloFunction()'  
    You should see Snowflake return the message: 'Hello World!'

You can now go modify and edit your `app.py`, `requirements.txt`, or other files and follow a similar flow, or update a function with `snow function update -n myfunction -f app.zip`

### Creating a Streamlit
1. Change to a directory with an existing streamlit app (or create one)
1. Run: `snow login` to select your snowsql config
1. Run: `snow configure` to create an environment and select your database, schema, role, and warehouse (environment name defaults to 'dev')
1. Run: `snow streamlit create <name>` to create a streamlit with a given name (file defaults to streamlit_app.py)
1. Run: `snow streamlit deploy <name> -o` to deploy your app and open it in the browser

## Future ideas
- Add delete command
- Add logs command (once logs ship)
- Add support for procedures
- Add support for notebooks (`nbconvert` to a stored procedure)
