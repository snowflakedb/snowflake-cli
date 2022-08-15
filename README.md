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
- Only currently works for Python functions (init, create, build, deploy, execute, describe)

## Installation

### Pre-requisites
- Python (written using Python 3.10)
- Pip
- Git

### Installation
Navigate to the directory you want to install in. Then run the following:

```bash
git clone https://github.com/jeffhollan/snowcli
cd snowcli
# you can also do the below in an active virtual environment:
# python -m venv .venv
# source .venv/bin/activate
pip install -r requirements.txt
pip install .
```

You should now be able to run `snowcli` and get the CLI message.

## Future ideas
- Add delete command
- Add logs command (once logs ship)
- Add support for procedures
- Add support for notebooks (`nbconvert` to a stored procedure)
