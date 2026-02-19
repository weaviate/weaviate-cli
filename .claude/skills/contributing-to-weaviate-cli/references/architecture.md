# Architecture Reference

Detailed breakdown of the weaviate-cli codebase structure and patterns.

## Entry Point: `cli.py`

The CLI entry point defines the top-level Click group with global options (`--config-file`, `--user`, `--version`) and registers all command groups:

```python
@click.group()
@click.option("--config-file", ...)
@click.option("--user", ...)
@click.option("--version", is_flag=True, callback=print_version, is_eager=True, ...)
@click.pass_context
def main(ctx, config_file, user):
    try:
        ctx.obj = {"config": ConfigManager(config_file, user)}
    except Exception as e:
        click.echo(f"Fatal Error: {e}")
        sys.exit(1)

main.add_command(create)
main.add_command(delete)
# ... etc
```

Key points:
- `ConfigManager` is created once and stored in `ctx.obj["config"]`
- Config creation is wrapped in try/except -- uses `"Fatal Error:"` prefix (distinct from the `"Error:"` prefix used in commands)
- Commands extract the client via `get_client_from_context(ctx)` from `utils.py`
- The `--version` flag uses an eager callback to print and exit

## Command Files: `weaviate_cli/commands/`

Each file defines a Click group and its subcommands. Pattern:

```python
@click.group()
def create():
    """Create resources in Weaviate."""
    pass

@create.command("collection")
@click.option("--collection", default=CreateCollectionDefaults.collection, ...)
@click.option("--json", "json_output", is_flag=True, default=False, ...)
@click.pass_context
def create_collection_cli(ctx, collection, ..., json_output):
    client = None
    try:
        client = get_client_from_context(ctx)
        manager = CollectionManager(client)
        manager.create_collection(collection=collection, ...)
    except Exception as e:
        click.echo(f"Error: {e}")
        if client:
            client.close()
        sys.exit(1)
    finally:
        if client:
            client.close()
```

Every command follows this structure:
1. Click decorators with options referencing defaults
2. `json_output` parameter (aliased from `--json`)
3. Try/except/finally with client cleanup
4. Delegate to manager for business logic

## Manager Files: `weaviate_cli/managers/`

Managers encapsulate business logic. Each takes a `WeaviateClient` in `__init__`:

```python
class CollectionManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def create_collection(self, collection, replication_factor, ...):
        if self.client.collections.exists(collection):
            raise Exception(f"Error: Collection '{collection}' already exists...")
        self.client.collections.create(name=collection, ...)
```

Managers handle:
- Input validation and error messages
- Weaviate client API calls
- Output formatting (JSON vs text via `print_json_or_text`)

## ConfigManager: `weaviate_cli/managers/config_manager.py`

Handles configuration loading and client creation:

- Default config path: `~/.config/weaviate/config.json`
- If no config file exists and none specified, creates default (localhost) config
- Supports three connection modes: local, Weaviate Cloud, custom host
- `get_client()` returns sync client, `get_async_client()` returns async client
- Handles `SLOW_CONNECTION` env var for doubled timeouts
- For `auth.type == "user"`, requires `--user` flag to select API key

## Defaults: `weaviate_cli/defaults.py`

Centralized defaults as dataclasses. One dataclass per command:

```python
@dataclass
class CreateCollectionDefaults:
    collection: str = "Movies"
    replication_factor: int = 3
    vector_index: str = "hnsw"
    # ...
```

Also contains:
- `PERMISSION_HELP_STRING` -- Multi-line help text for `-p` flag
- `QUERY_MAXIMUM_RESULTS` -- Hard limit for query results
- `MAX_OBJECTS_PER_BATCH` -- Batch size cap
- `MAX_WORKERS` -- Computed from CPU count (capped at 32)

## Utils: `weaviate_cli/utils.py`

Shared utility functions:

| Function | Purpose |
|----------|---------|
| `get_client_from_context(ctx)` | Extract sync client from Click context |
| `get_async_client_from_context(ctx)` | Extract async client from Click context |
| `print_json_or_text(data, json_output, text_fn)` | Output JSON or formatted text |
| `pp_objects(response, properties, json_output)` | Pretty-print query results |
| `parse_permission(perm)` | Parse permission string to RBAC objects |
| `older_than_version(client, version)` | Check if Weaviate server is older than given version |
| `is_version_older_than(version, check_version)` | Compare two semver strings |
| `get_random_string(length)` | Random string generator |

## Completion: `weaviate_cli/completion/`

Shell completion helpers for Click options:
- `collection_name_complete` -- Auto-complete collection names
- `role_name_complete` -- Auto-complete role names

## Types: `weaviate_cli/types/`

Type definitions used across the codebase.

## Datasets: `weaviate_cli/datasets/`

Built-in datasets (Movies) used for non-randomized data ingestion.
