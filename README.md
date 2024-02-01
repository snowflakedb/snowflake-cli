# Snowflake Developer CLI

**Note**: Snowflake CLI is in Private Preview (PrPr). You must register for the PrPr to use Snowflake CLI by filling out the
[Snowflake CLI  - PrPr Intake Form](https://forms.gle/HZNhPNbzn7oExjFu8). Also, if you want to access Snowflake Container
Services through Snowflake CLI, you must register for its PrPr. For more information, you can contact a
Snowflake sales representative.

## Install Snowflake CLI

### Install with pip (PyPi)

Requires Python >= 3.8

```bash
pip install snowflake-cli-labs
snow --help
```

### Install with Homebrew (Mac only)

Requires [Homebrew](https://brew.sh/).

```bash
brew tap Snowflake-Labs/snowcli
brew install snowcli
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
