# weaviate-cli

CLI tool for managing Weaviate vector databases. Built with Python + Click.

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
make install-dev    # Install deps + pre-commit hooks
make test           # Run unit tests
```

## Development Commands

| Command | What it does |
|---------|-------------|
| `make format` | Black formatter on cli.py, weaviate_cli/, test/ |
| `make lint` | Black --check (CI-equivalent) |
| `make test` | pytest test/unittests |
| `make build-all` | Build package + twine check |
| `make all` | format + lint + test + build-all |

## Architecture

`cli.py` (Click group) -> `weaviate_cli/commands/` (Click decorators) -> `weaviate_cli/managers/` (business logic)

- **defaults.py**: Dataclass defaults for every command (always reference these, never hardcode)
- **utils.py**: `get_client_from_context()`, `print_json_or_text()`, `parse_permission()`
- **ConfigManager**: Created once in `cli.py`, stored in `ctx.obj["config"]`

## Key Conventions

- Every command **must** have `--json` support (parameter named `json_output`)
- Defaults live in `defaults.py` as dataclasses
- Black formatting (default settings, 88 char line length)
- Client lifecycle: get in `try`, close in `finally`, `sys.exit(1)` on error
- Error format: `click.echo(f"Error: {e}")`

## Testing

- **Unit tests**: `test/unittests/` -- CliRunner for CLI, MagicMock for managers
- **Integration tests**: `test/integration/` -- requires running Weaviate cluster
- **CI**: lint -> unit tests (Python 3.9-3.13) -> integration tests (latest Weaviate)

## Agent Skills

Skills live in `.claude/skills/`:
- `operating-weaviate-cli/` -- Using the CLI to manage Weaviate clusters
- `contributing-to-weaviate-cli/` -- Developing and testing the CLI codebase

When adding new commands or options, update both the code and the relevant skill documentation.
