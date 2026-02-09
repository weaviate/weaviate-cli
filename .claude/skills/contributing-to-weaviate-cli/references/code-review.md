# Code Review Reference

PR checklist, common pitfalls, and conventions for weaviate-cli.

## PR Checklist

### Required for All PRs

- [ ] `make lint` passes (Black formatting)
- [ ] `make test` passes (unit tests)
- [ ] No hardcoded paths or credentials
- [ ] No debug/print statements left in code

### Required for New Commands

- [ ] `--json` flag with `json_output` parameter name
- [ ] Defaults dataclass in `defaults.py`
- [ ] Manager method with `print_json_or_text()` for output
- [ ] Client properly closed in `finally` block
- [ ] Error messages follow `Error: <description>` pattern
- [ ] Unit test for happy path
- [ ] Unit test for error path (e.g., missing collection)
- [ ] Operating skill documentation updated (SKILL.md + reference file)

### Required for New Options

- [ ] Default value from defaults dataclass (not hardcoded)
- [ ] Help text includes default value
- [ ] `type=click.Choice([...])` for constrained values
- [ ] Parameter passed through command -> manager
- [ ] Unit test covers the new option

## Common Pitfalls

### 1. Shadowing `json` Module

Wrong:
```python
def my_function(json: bool):  # shadows json module
    import json  # won't work
```

Right:
```python
@click.option("--json", "json_output", is_flag=True, ...)
def my_function(json_output: bool):
    import json  # works fine
```

### 2. Client Not Closed on Error

Wrong:
```python
client = get_client_from_context(ctx)
manager = SomeManager(client)
manager.do_something()  # if this raises, client leaks
client.close()
```

Right:
```python
client = None
try:
    client = get_client_from_context(ctx)
    manager = SomeManager(client)
    manager.do_something()
except Exception as e:
    click.echo(f"Error: {e}")
    if client:
        client.close()
    sys.exit(1)
finally:
    if client:
        client.close()
```

### 3. Hardcoded Defaults

Wrong:
```python
@click.option("--collection", default="Movies", ...)
```

Right:
```python
@click.option("--collection", default=CreateCollectionDefaults.collection, ...)
```

### 4. Missing `sys.exit(1)` on Error

Commands must exit with code 1 on error for proper scripting support. Don't just echo the error.

### 5. Not Passing `json_output` to Manager

Every manager method that produces output should accept and use `json_output`.

## Code Conventions

### Formatting

- **Black** formatter with default settings
- No custom line length -- Black's default (88 chars)
- Run `make format` before committing

### Naming

- Command functions: `<action>_<resource>_cli` (e.g., `create_collection_cli`)
- Manager classes: `<Resource>Manager` (e.g., `CollectionManager`)
- Manager methods: `<action>_<resource>` (e.g., `create_collection`)
- Defaults classes: `<Action><Resource>Defaults` (e.g., `CreateCollectionDefaults`)
- Test functions: `test_<action>_<resource>[_<scenario>]` (e.g., `test_create_existing_collection`)

### Error Messages

- Command level: `click.echo(f"Error: {e}")`
- Manager level: `raise Exception(f"Description of what went wrong.")`
- Validation: `click.echo("Error: --flag has no effect unless ..."); sys.exit(1)`

### Git Workflow

- Branch naming: `<username>/<feature-description>`
- Main branch: `master`
- PRs require CI passing
- Pre-commit hooks: `make install-dev` sets up Black
