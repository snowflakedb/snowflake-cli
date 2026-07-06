#!/usr/bin/env bash
# Project-specific mirroring configuration.
# This is the only file that needs to change when adapting these scripts to a
# different project. All other scripts derive their project-specific values
# from variables defined here.

SOURCE_ORG="snowflake-eng"
SOURCE_REPO="snowflake-cli"

MIRROR_ORG="snowflakedb"
MIRROR_REPO="snowflake-cli"

MIRROR_BOT_NAME="Mirror Bot"
MIRROR_BOT_EMAIL="mirror-bot@snowflake.com"

# Copybara workflow name — must match the name() in copy.bara.sky
COPYBARA_WORKFLOW="mirror"

# Copybara JAR release — must match the ARG default in copybara/Dockerfile.
# Can be overridden via environment variable without editing this file.
COPYBARA_RELEASE="${COPYBARA_RELEASE:-v20260504}"
