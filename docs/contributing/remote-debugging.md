<!--
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# Remote Debugging with PyCharm or IntelliJ

Snowflake CLI can connect to a remote debug server started in PyCharm or
IntelliJ, allowing you to debug any installation of the tool.

## Requirements

- The same source code loaded in your IDE as running in the debugged CLI installation
- Open network connection to your IDE
- `pydevd-pycharm.egg` file accessible on the machine where the CLI is installed
  (must match your IDE version)

## Setup

1. Create a "remote debug config" run configuration following
   [steps 1-2 from the JetBrains tutorial](https://www.jetbrains.com/help/pycharm/remote-debugging-with-product.html#create-remote-debug-config).
   The defaults (`localhost`, port `12345`) match the CLI defaults.
2. Run your configuration in debug mode.
3. Find `pydevd-pycharm.egg` in your IDE installation directory. Tips on
   locating it appear in the JetBrains tutorial and in the run configuration
   creation window.
4. If the CLI and IDE are on the same machine, copy the path to the file. If the
   CLI is on another machine, copy the file there and note the path.
5. Run the CLI with:
   ```bash
   snow --pycharm-debug-library-path <path-to-pydevd-pycharm.egg> <command>
   ```
   Example:
   ```bash
   snow --pycharm-debug-library-path \
     "/Users/xyz/Library/Application Support/JetBrains/Toolbox/apps/IDEA-U/ch-0/231.9011.34/IntelliJ IDEA.app.plugins/python/debugger-eggs-output/pydevd-pycharm.egg" \
     snowpark function list
   ```

## Options

- `--pycharm-debug-server-host` — override the debug server host (default: `localhost`)
- `--pycharm-debug-server-port` — override the debug server port (default: `12345`)

Code execution pauses before your command runs. You can resume, add breakpoints,
and evaluate variables in the IDE's debug view.
