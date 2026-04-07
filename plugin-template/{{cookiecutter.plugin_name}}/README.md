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
