#!/bin/bash -e

BASEDIR=$(dirname $0)
osascript <<APPL_SCRIPT
tell application "Terminal"
    if not (exists window 1) then reopen
    do script "$BASEDIR/snow" in the last window
    set the bounds of the last window to {0, 0, 1400, 800}
    activate
end tell
APPL_SCRIPT
