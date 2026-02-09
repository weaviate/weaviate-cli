---
name: contributing-to-weaviate-cli
description: >-
  Develops, tests, and reviews weaviate-cli source code. Use this skill when
  the task involves modifying the weaviate-cli codebase itself. Trigger if
  the request: asks to add a new command or flag to weaviate-cli; needs to
  fix a bug in the CLI source code (weaviate_cli/ Python files); involves
  reviewing a PR that changes weaviate-cli; wants to write or update unit or
  integration tests for the CLI; asks about the Click-based architecture,
  command-to-manager pattern, or defaults system; or the working directory is
  the weaviate-cli repository and Python source files need to be modified.
  Do NOT use for running weaviate-cli commands against a Weaviate cluster —
  use operating-weaviate-cli for that.
---

# Contributing to weaviate-cli

Guide for developing, testing, and maintaining the `weaviate-cli` codebase.

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
make install-dev          # pip install -r requirements-dev.txt + pre-commit install
```

Key development commands:
```bash
make format               # Black formatter on cli.py, weaviate_cli/, test/
make lint                 # Black --check (CI-equivalent)
make test                 # pytest test/unittests
make build-all            # python -m build + twine check
make all                  # format + lint + test + build-all
```

## Repository Architecture

```
weaviate-cli/
  cli.py                        # Entry point: Click group, global options, command registration
  weaviate_cli/
    __init__.py                 # Package version
    defaults.py                 # Dataclass defaults for all commands
    utils.py                    # Shared helpers: get_client, pp_objects, parse_permission
    commands/                   # Click command definitions (one file per group)
      create.py                 # create collection, tenants, data, backup, role, user, alias, replication
      get.py                    # get collection, tenants, shards, backup, role, user, nodes, alias, replication
      update.py                 # update collection, tenants, shards, data, user, alias
      delete.py                 # delete collection, tenants, data, role, user, alias, replication
      query.py                  # query data, replications, sharding-state
      restore.py                # restore backup
      cancel.py                 # cancel backup, replication
      assign.py                 # assign role, permission
      revoke.py                 # revoke role, permission
      benchmark.py              # benchmark qps
    managers/                   # Business logic (one file per domain)
      config_manager.py         # ConfigManager: config loading, client creation
      collection_manager.py     # CollectionManager: CRUD collections
      tenant_manager.py         # TenantManager: CRUD tenants
      data_manager.py           # DataManager: ingest/update/delete data
      backup_manager.py         # BackupManager: backup/restore operations
      role_manager.py           # RoleManager: RBAC role operations
      user_manager.py           # UserManager: RBAC user operations
      node_manager.py           # NodeManager: node information
      shard_manager.py          # ShardManager: shard operations
      cluster_manager.py        # ClusterManager: replication operations
      alias_manager.py          # AliasManager: alias operations
      benchmark_manager.py      # BenchmarkManager: QPS benchmarks
    completion/                 # Shell completion helpers
    datasets/                   # Built-in datasets (Movies)
    types/                      # Type definitions
  test/
    unittests/
      conftest.py               # Fixtures: mock_client, mock_config, mock_click_context
      test_cli.py               # CliRunner tests for CLI entry point
      test_defaults.py          # Defaults dataclass tests
      test_utils.py             # Utility function tests
      test_managers/             # Manager unit tests (one per manager)
    integration/
      test_integration.py       # Integration tests (requires running cluster)
      test_auth_integration.py  # RBAC integration tests (requires cluster + RBAC)
      test_data_integration.py  # Data integration tests
```

See [references/architecture.md](references/architecture.md) for detailed class hierarchy and patterns.

## Architecture Pattern: Commands -> Managers

Every CLI operation follows this pattern:

1. **`cli.py`**: Top-level Click group registers command groups
2. **`commands/<group>.py`**: Click decorators define options, validate input, get client from context, call manager
3. **`managers/<domain>_manager.py`**: Business logic, Weaviate client calls, output formatting
4. **`defaults.py`**: Dataclass with default values for each command

```
cli.py (main group)
  -> commands/create.py (create group)
    -> @create.command("collection") (Click decorators + options)
      -> CollectionManager(client).create_collection(...) (business logic)
        -> defaults.py::CreateCollectionDefaults (default values)
```

The `ConfigManager` is stored in the Click context (`ctx.obj["config"]`). Commands extract the client via `get_client_from_context(ctx)` from `utils.py`.

## Defaults System

All command defaults live in `weaviate_cli/defaults.py` as dataclasses:

```python
@dataclass
class CreateCollectionDefaults:
    collection: str = "Movies"
    replication_factor: int = 3
    vector_index: str = "hnsw"
    multitenant: bool = False
    # ... etc
```

Click options reference these: `default=CreateCollectionDefaults.collection`. This keeps defaults centralized and testable.

When adding a new command, always create a corresponding defaults dataclass.

## Adding a New Command

Step-by-step guide: see [references/adding-commands.md](references/adding-commands.md).

Summary:
1. Add a defaults dataclass in `defaults.py`
2. Add Click command in the appropriate `commands/<group>.py`
3. Add manager method in `managers/<domain>_manager.py`
4. Add `--json` flag (mandatory for all commands)
5. Add unit test in `test/unittests/test_managers/`
6. Update the operating skill documentation

## JSON Output Convention

Every command **must** support `--json`:

**In the Click command:**
```python
@click.option("--json", "json_output", is_flag=True, default=False, help="Output in JSON format.")
```

Note: The parameter is named `json_output` (not `json`) to avoid shadowing Python's `json` module.

**In the manager**, use `print_json_or_text()` from `utils.py`:
```python
from weaviate_cli.utils import print_json_or_text

print_json_or_text(
    data={"collections": collection_list, "total": len(collection_list)},
    json_output=json_output,
    text_fn=lambda: click.echo(formatted_text),
)
```

**Success JSON shape:**
```json
{"status": "success", "message": "..."}
```
or structured data with domain-specific fields.

**Error output:** Always `click.echo(f"Error: {e}")` + `sys.exit(1)`.

## Testing

### Unit Tests

Use `CliRunner` for CLI-level tests and `MagicMock` for manager tests:

```python
# CLI test (test/unittests/test_cli.py)
from click.testing import CliRunner
from cli import main

def test_create_collection(cli_runner):
    result = cli_runner.invoke(main, ["create", "collection", "--json"])
    assert result.exit_code == 0

# Manager test (test/unittests/test_managers/test_collection_manager.py)
def test_create_collection(mock_client):
    mock_client.collections.exists.side_effect = [False, True]
    manager = CollectionManager(mock_client)
    manager.create_collection(collection="Test", replication_factor=3)
    mock_client.collections.create.assert_called_once()
```

Fixtures in `conftest.py` (shared across all unit tests):
- `mock_client` -- `MagicMock(spec=weaviate.WeaviateClient)`
- `mock_config` -- `MagicMock(spec=ConfigManager)`
- `mock_click_context` -- context with mock config in `ctx.obj`

Note: `cli_runner` is defined locally in `test/unittests/test_cli.py`, not in `conftest.py`.

### Integration Tests

Require a running Weaviate cluster (typically via `weaviate-local-k8s`):
```bash
pytest test/integration/test_integration.py
pytest test/integration/test_auth_integration.py  # Requires RBAC-enabled cluster
```

See [references/testing.md](references/testing.md) for full patterns.

## CI Pipeline

```
lint-and-format (Black + build check)
  -> unit-tests (Python 3.9-3.13, matrix)
    -> integration-tests (latest Weaviate, weaviate-local-k8s, Python 3.9-3.13)
    -> integration-auth-tests (RBAC-enabled cluster, Python 3.9-3.13)
```

- Linting must pass before unit tests run
- Unit tests must pass before integration tests run
- Integration tests use `weaviate/weaviate-local-k8s@v2` GitHub Action
- Auth integration tests create a config with `admin-key` API key

## Code Review Checklist

1. New command has `--json` support
2. Defaults dataclass added/updated in `defaults.py`
3. Unit test covers happy path and error cases
4. Black formatting passes (`make lint`)
5. No hardcoded paths or credentials
6. Error messages follow `Error: <description>` pattern
7. Client is properly closed in `finally` block
8. Manager method handles both JSON and text output

See [references/code-review.md](references/code-review.md) for detailed checklist.

## Maintaining Skills

When adding new commands or options to `weaviate-cli`, update the agent skills:

1. **Operating skill** (`.claude/skills/operating-weaviate-cli/`):
   - Add the new command to the **Command Reference** section in `SKILL.md`
   - Update or create the relevant reference file in `references/`
   - Update the **Command Groups** table if a new group is added
   - Update **Workflow Dependencies** if the command introduces new dependencies

2. **Contributing skill** (`.claude/skills/contributing-to-weaviate-cli/`):
   - Update `references/architecture.md` if new files/modules are added
   - Update `references/adding-commands.md` if patterns change
   - Update `references/testing.md` if test infrastructure changes

3. **CLAUDE.md**: Update if development commands or conventions change

## References

- [references/architecture.md](references/architecture.md) -- File-by-file breakdown, class hierarchy, utils helpers
- [references/adding-commands.md](references/adding-commands.md) -- Complete worked example for new commands
- [references/testing.md](references/testing.md) -- Test fixtures, patterns, CI details
- [references/code-review.md](references/code-review.md) -- PR checklist, common pitfalls, conventions
