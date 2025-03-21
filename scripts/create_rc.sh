#Parse the version argument
SEMVER_REGEX="^[0-9]+\.[0-9]+\.[0-9]+$"


#Main variables:
SCRIPT_DIR=$(dirname "$(realpath "$0")")
MAIN_REPO_DIR=$(dirname "$SCRIPT_DIR")

#Change dir to main repo dir
cd $MAIN_REPO_DIR
VERSION=$(cat "src/snowflake/cli/__about__.py"| grep VERSION | sed -E 's/.*"([0-9]+\.[0-9]+\.[0-9]+)\.dev0".*/\1/')
echo $VERSION

#Make sure we`re up to date with main
#git fetch -all
#git checkout origin/main

#Run the update release-notes script

