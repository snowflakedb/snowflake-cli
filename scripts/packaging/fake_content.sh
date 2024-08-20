#!/bin/zsh

# COMMIT_SHA=$(git rev-parse HEAD)
# COMMIT_SHA="4bbeea80e333d675506c8956c9bea469dd7a800c"
# VERSION="2.7.99"
# RELEASE_TYPE="release"
# PLATFORMS=("linux_aarch64" "linux_x86_64" "mac_arm64" "windows_x86_64")
#
# FILES=("darwin-arm64.pkg" "aarch64.deb" "aarch64.rpm" "x86_64.deb" "x86_64.rpm" "x86_64.msi")
#
# for platfom in $PLATFORMS; do
#   REPO_URL="s3://sfc-eng-jenkins/repository/snowflake-cli/${RELEASE_TYPE}/${platfom}/${COMMIT_SHA}/"
#   echo $REPO_URL
#
#   for f in $FILES; do
#     _file="snowflake-cli-${VERSION}-${f}"
#     echo $_file
#   done
#
# done
# 5a80e195094ec8faa538ce0693102d013cfb9195/4bbeea80e333d675506c8956c9bea469dd7a800c
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/linux_aarch64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-aarch64.deb
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/linux_aarch64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-aarch64.rpm
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/linux_x86_64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-x86_64.deb
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/linux_x86_64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-x86_64.rpm
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/mac_arm64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-darwin-arm64.pkg
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/windows_x86_64/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake-cli-2.7.99-x86_64.msi
aws s3 cp fake_bin s3://sfc-eng-jenkins/repository/snowflake-cli/release/python/4bbeea80e333d675506c8956c9bea469dd7a800c/snowflake_cli_labs-2.7.99-py3-none-any.whl
