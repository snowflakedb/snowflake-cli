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

# {{ cookiecutter.plugin_name }}

{{ cookiecutter.plugin_description }}

## Development

### Install in development mode

```bash
pip install -e .
```

### Enable the plugin

```bash
snow plugin enable {{ cookiecutter.plugin_module }}
```

### Run tests

```bash
pytest
```

## Plugin Structure

| File | Purpose |
|------|---------|
| `interface.py` | Command surface definition (spec + handler ABC) -- reviewed in Phase 1 |
| `handler.py` | Business logic implementation -- reviewed in Phase 2 |
| `plugin_spec.py` | Wires interface + handler into Snowflake CLI |

## Contributing

1. Modify `interface.py` to define your command surface (Phase 1 PR)
2. After approval, implement handlers in `handler.py` (Phase 2 PR)
3. Run `pytest` to validate the interface-handler contract
