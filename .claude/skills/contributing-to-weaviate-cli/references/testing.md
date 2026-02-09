# Testing Reference

Unit test and integration test patterns for weaviate-cli.

## Running Tests

```bash
make test                                          # All unit tests
pytest test/unittests                              # Same as above
pytest test/unittests/test_managers/test_collection_manager.py  # Single file
pytest test/unittests -k "test_create_collection"  # Pattern match
pytest test/integration/test_integration.py        # Integration (needs cluster)
pytest test/integration/test_auth_integration.py   # RBAC integration (needs cluster + RBAC)
```

## Fixtures (`test/unittests/conftest.py`)

```python
@pytest.fixture
def mock_client():
    """MagicMock with WeaviateClient spec -- use for manager tests."""
    return MagicMock(spec=weaviate.WeaviateClient)

@pytest.fixture
def mock_config():
    """MagicMock with ConfigManager spec."""
    return MagicMock(spec=ConfigManager)

@pytest.fixture
def mock_click_context(mock_config):
    """Mock Click context with config in ctx.obj."""
    ctx = MagicMock()
    ctx.obj = {"config": mock_config}
    return ctx
```

## Unit Test Patterns

### Manager Tests

Test managers directly using `mock_client`:

```python
def test_create_collection(mock_client):
    # 1. Setup mock chain
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.side_effect = [False, True]  # doesn't exist, then exists after create

    # 2. Create manager and call method
    manager = CollectionManager(mock_client)
    manager.create_collection(
        collection="TestCollection",
        replication_factor=3,
        vector_index="hnsw",
        async_enabled=True,
    )

    # 3. Assert calls
    assert mock_collections.exists.call_count == 2
    mock_collections.create.assert_called_once()

    # 4. Verify parameters
    create_call_kwargs = mock_collections.create.call_args.kwargs
    assert create_call_kwargs["name"] == "TestCollection"
    assert create_call_kwargs["replication_config"].factor == 3
```

### CLI Tests

Test CLI commands using `CliRunner`. Note: the `cli_runner` fixture is defined locally in `test/unittests/test_cli.py`, not in `conftest.py`.

```python
# In test/unittests/test_cli.py
from click.testing import CliRunner
from cli import main

@pytest.fixture
def cli_runner():
    return CliRunner()

def test_main_help(cli_runner):
    result = cli_runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "Usage:" in result.output

def test_invalid_command(cli_runner):
    result = cli_runner.invoke(main, ["invalid_command"])
    assert result.exit_code == 2
    assert "No such command" in result.output
```

### Error Path Tests

Always test error conditions:

```python
def test_create_existing_collection(mock_client):
    mock_client.collections.exists.return_value = True
    manager = CollectionManager(mock_client)

    with pytest.raises(Exception) as exc_info:
        manager.create_collection(collection="TestCollection")

    assert "already exists" in str(exc_info.value)
    mock_client.collections.create.assert_not_called()
```

### Mock Chain Patterns

Common mock chains for the Weaviate client:

```python
# Collections
mock_client.collections.exists.return_value = True
mock_client.collections.create.return_value = MagicMock()
mock_client.collections.get.return_value = mock_collection
mock_client.collections.list_all.return_value = {"Movies": mock_config}
mock_client.collections.delete.return_value = None

# Tenants
mock_collection = MagicMock()
mock_client.collections.get.return_value = mock_collection
mock_collection.tenants.get.return_value = {"Tenant-0": mock_tenant}
mock_collection.tenants.create.return_value = None

# Nodes
mock_client.cluster.nodes.return_value = [mock_node]

# Backups
mock_client.backup.create.return_value = mock_backup_response
```

## Test Coverage

Managers with unit tests in `test/unittests/test_managers/`:
- `AliasManager` — create, update, delete, get, print (json + text)
- `BackupManager` — create, restore, get, cancel (json + text, error cases)
- `ClusterManager` — replication CRUD, print_replication/print_replications (json + text)
- `CollectionManager` — create, update, delete, get
- `ConfigManager` — config loading, client creation
- `DataManager` — create, update, delete (basic happy path)
- `NodeManager` — get nodes (json + text)
- `RoleManager` — create, delete, add_permission, revoke_permission (json + text)
- `ShardManager` — get, update shards
- `TenantManager` — create, delete, get, update tenants (json + text, version branching)
- `UserManager` — create, update, delete, add_role, revoke_role, print helpers

Manager **without** unit tests:
- `BenchmarkManager` — async + complex output; covered by integration tests

## Integration Tests

### Requirements

- Running Weaviate cluster (typically via `weaviate-local-k8s`)
- For `test_auth_integration.py`: RBAC-enabled cluster with config at `~/.config/weaviate/config.json`

### CI Setup

Integration tests in CI use the `weaviate/weaviate-local-k8s@v2` GitHub Action:

```yaml
- uses: weaviate/weaviate-local-k8s@v2
  with:
    workers: 1
    replicas: 1
    weaviate-version: ${{ env.WEAVIATE_VERSION }}
    modules: "text2vec-transformers-model2vec,text2vec-contextionary"
    enable-backup: true
    dynamic-users: true      # For user management tests
    rbac: true               # For auth integration tests
```

### Local Integration Testing

To run integration tests locally:

```bash
# Start a local cluster (e.g., using weaviate-local-k8s or docker-compose)
# Then run:
pytest test/integration/test_integration.py
```

For auth tests, create a config first:
```bash
mkdir -p ~/.config/weaviate
echo '{"host":"localhost","http_port":"8080","grpc_port":"50051","auth":{"type":"api_key","api_key":"admin-key"}}' > ~/.config/weaviate/config.json
pytest test/integration/test_auth_integration.py
```

## CI Pipeline Details

The CI runs in GitHub Actions (`.github/workflows/main.yaml`):

1. **lint-and-format**: Black check + build/twine check (Python 3.11)
2. **unit-tests** (needs lint): Matrix across Python 3.9-3.13, generates HTML reports
3. **integration-tests** (needs unit): Latest Weaviate version, modules enabled, backup enabled
4. **integration-auth-tests** (needs unit): Same as integration but with RBAC enabled

All test jobs upload HTML test reports as artifacts.
