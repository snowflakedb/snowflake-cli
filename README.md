# Snowflake Developer CLI

**Note**: Snowflake CLI is in Private Preview (PrPr). You must register for the PrPr to use Snowflake CLI by filling out the
[Snowflake CLI  - PrPr Intake Form](https://forms.gle/HZNhPNbzn7oExjFu8). Also, if you want to access Snowflake Container
Services through Snowflake CLI, you must register for its PrPr. For more information, you can contact a
Snowflake sales representative.

For complete installation and usage instructions, refer to the
**[Snowflake CLI Guide](https://docs.snowflake.com/LIMITEDACCESS/snowcli/snowcli-guide)**.

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

- To run Streamlit in Snowflake using Snowflake CLI, your Snowflake account must have access to the Streamlit public preview.

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

## Get involved

Have a feature idea? Running into a bug? Want to contribute? We'd love to hear from you!
Please open or review issues, open pull requests, or reach out to us on developers@snowflake.com
