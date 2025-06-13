SCRIPT_DIR=$(dirname "$0")

cd "$SCRIPT_DIR/echo_service"
source "build_and_push.sh"
cd "../test_counter"
source "build_and_push.sh"
