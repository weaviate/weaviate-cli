import pytest
from weaviate_cli.defaults import *
from weaviate.collections.classes.tenants import TenantActivityStatus
from weaviate_cli.commands.create import (
    create_collection_cli,
    create_tenants_cli,
    create_backup_cli,
    create_data_cli,
)
from weaviate_cli.commands.delete import (
    delete_collection_cli,
    delete_tenants_cli,
    delete_data_cli,
)
from weaviate_cli.commands.get import (
    get_collection_cli,
    get_tenants_cli,
    get_shards_cli,
    get_backup_cli,
)
from weaviate_cli.commands.query import query_data_cli
from weaviate_cli.commands.restore import restore_backup_cli
from weaviate_cli.commands.update import (
    update_collection_cli,
    update_tenants_cli,
    update_shards_cli,
    update_data_cli,
)
from click.testing import CliRunner
from unittest.mock import MagicMock
import click


@pytest.fixture
def runner():
    return CliRunner()


def test_create_collection_defaults(runner):
    # Create mock context with config
    ctx = click.Context(create_collection_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    # Mock exists() to return False so create_collection succeeds
    mock_client = MagicMock()
    mock_client.collections.exists.side_effect = [False, True]
    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(create_collection_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert CreateCollectionDefaults.collection == CreateCollectionDefaults.collection
    assert (
        CreateCollectionDefaults.replication_factor
        == CreateCollectionDefaults.replication_factor
    )
    assert (
        CreateCollectionDefaults.async_enabled == CreateCollectionDefaults.async_enabled
    )
    assert (
        CreateCollectionDefaults.vector_index == CreateCollectionDefaults.vector_index
    )
    assert (
        CreateCollectionDefaults.training_limit
        == CreateCollectionDefaults.training_limit
    )
    assert CreateCollectionDefaults.multitenant == CreateCollectionDefaults.multitenant
    assert CreateCollectionDefaults.shards == CreateCollectionDefaults.shards


def test_create_tenants_defaults(runner):
    # Create mock context with config
    ctx = click.Context(create_tenants_cli)
    ctx.obj = {"config": MagicMock()}
    mock_client = MagicMock()

    # Mock exists() to return True so create_collection succeeds
    mock_client.collections.exists.return_value = True
    mock_client.get_meta.return_value = {"version": "1.25.0"}

    # Mock collection object
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Mock multi-tenancy config
    mock_config = MagicMock()
    mock_config.multi_tenancy_config.enabled = True
    mock_collection.config.get.return_value = mock_config

    # Mock tenants with side effect to return different values on subsequent calls
    empty_tenants = {}
    mock_collection.tenants.get.return_value = empty_tenants

    mock_tenants = {
        f"{CreateTenantsDefaults.tenant_suffix}{i}": MagicMock(
            activity_status=TenantActivityStatus.ACTIVE
        )
        for i in range(CreateTenantsDefaults.number_tenants)
    }
    mock_collection.tenants.get_by_names.return_value = mock_tenants
    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(create_tenants_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert CreateTenantsDefaults.collection == CreateTenantsDefaults.collection
    assert CreateTenantsDefaults.tenant_suffix == CreateTenantsDefaults.tenant_suffix
    assert CreateTenantsDefaults.number_tenants == CreateTenantsDefaults.number_tenants
    assert CreateTenantsDefaults.state == CreateTenantsDefaults.state


def test_create_backup_defaults(runner):
    # Create mock context with config
    ctx = click.Context(create_backup_cli)
    ctx.obj = {"config": MagicMock()}
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = True
    mock_client.get_meta.return_value = {"version": "1.24.0"}
    mock_client.backup.create.return_value = MagicMock(
        status=MagicMock(value="SUCCESS")
    )
    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(create_backup_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert CreateBackupDefaults.backend == CreateBackupDefaults.backend
    assert CreateBackupDefaults.backup_id == CreateBackupDefaults.backup_id
    assert CreateBackupDefaults.include == CreateBackupDefaults.include
    assert CreateBackupDefaults.exclude == CreateBackupDefaults.exclude
    assert CreateBackupDefaults.wait == CreateBackupDefaults.wait
    assert CreateBackupDefaults.cpu_for_backup == CreateBackupDefaults.cpu_for_backup


def test_create_data_defaults(runner):
    # Create mock context with config
    ctx = click.Context(create_data_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(create_data_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert CreateDataDefaults.collection == CreateDataDefaults.collection
    assert CreateDataDefaults.limit == CreateDataDefaults.limit
    assert CreateDataDefaults.consistency_level == CreateDataDefaults.consistency_level
    assert CreateDataDefaults.randomize == CreateDataDefaults.randomize
    assert CreateDataDefaults.auto_tenants == CreateDataDefaults.auto_tenants
    assert CreateDataDefaults.vector_dimensions == CreateDataDefaults.vector_dimensions


def test_delete_collection_defaults(runner):
    # Create mock context with config
    ctx = click.Context(delete_collection_cli)
    ctx.obj = {"config": MagicMock()}
    mock_client = MagicMock()
    mock_client.collections.exists.side_effect = [True, False]
    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(delete_collection_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert DeleteCollectionDefaults.collection == DeleteCollectionDefaults.collection
    assert DeleteCollectionDefaults.all == DeleteCollectionDefaults.all


def test_delete_tenants_defaults(runner):
    # Create mock context with config
    ctx = click.Context(delete_tenants_cli)
    ctx.obj = {"config": MagicMock()}
    mock_client = MagicMock()
    mock_client.get_meta.return_value = {"version": "1.25.0"}
    mock_client.collections.exists.return_value = True

    # Mock collection object
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Mock multi-tenancy config
    mock_config = MagicMock()
    mock_config.multi_tenancy_config.enabled = True
    mock_collection.config.get.return_value = mock_config

    # Mock tenants with side effect to return different values on subsequent calls
    mock_tenants = {
        f"{DeleteTenantsDefaults.tenant_suffix}{i}": MagicMock()
        for i in range(1, DeleteTenantsDefaults.number_tenants + 1)
    }
    empty_tenants = {}
    mock_collection.tenants.get.side_effect = [mock_tenants, empty_tenants]

    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(delete_tenants_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert DeleteTenantsDefaults.collection == DeleteTenantsDefaults.collection
    assert DeleteTenantsDefaults.tenant_suffix == DeleteTenantsDefaults.tenant_suffix
    assert DeleteTenantsDefaults.number_tenants == DeleteTenantsDefaults.number_tenants


def test_delete_data_defaults(runner):
    # Create mock context with config
    ctx = click.Context(delete_data_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(delete_data_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert DeleteDataDefaults.collection == DeleteDataDefaults.collection
    assert DeleteDataDefaults.limit == DeleteDataDefaults.limit
    assert DeleteDataDefaults.consistency_level == DeleteDataDefaults.consistency_level
    assert DeleteDataDefaults.uuid == DeleteDataDefaults.uuid


def test_get_collection_defaults(runner):
    # Create mock context with config
    ctx = click.Context(get_collection_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(get_collection_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert GetCollectionDefaults.collection == GetCollectionDefaults.collection


def test_get_tenants_defaults(runner):
    # Create mock context with config
    ctx = click.Context(get_tenants_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(get_tenants_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert GetTenantsDefaults.collection == GetTenantsDefaults.collection
    assert GetTenantsDefaults.verbose == GetTenantsDefaults.verbose


def test_get_shards_defaults(runner):
    # Create mock context with config
    ctx = click.Context(get_shards_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(get_shards_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert GetShardsDefaults.collection == GetShardsDefaults.collection


def test_get_backup_defaults(runner):
    # Create mock context with config
    ctx = click.Context(get_backup_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    # Mock backup manager
    backup_manager = MagicMock()
    backup_manager.get_backup = MagicMock()

    # Mock client
    client = MagicMock()
    client.backup = backup_manager
    ctx.obj["config"].get_client.return_value = client

    result = runner.invoke(get_backup_cli, ["--backup_id", "test-backup"], obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert GetBackupDefaults.backend == GetBackupDefaults.backend
    assert GetBackupDefaults.backup_id == GetBackupDefaults.backup_id
    assert GetBackupDefaults.restore == GetBackupDefaults.restore


def test_query_data_defaults(runner):
    # Create mock context with config
    ctx = click.Context(query_data_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(query_data_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert QueryDataDefaults.collection == QueryDataDefaults.collection
    assert QueryDataDefaults.search_type == QueryDataDefaults.search_type
    assert QueryDataDefaults.query == QueryDataDefaults.query
    assert QueryDataDefaults.consistency_level == QueryDataDefaults.consistency_level
    assert QueryDataDefaults.limit == QueryDataDefaults.limit
    assert QueryDataDefaults.properties == QueryDataDefaults.properties


def test_restore_backup_defaults(runner):
    # Create mock context with config
    ctx = click.Context(restore_backup_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(restore_backup_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert RestoreBackupDefaults.backend == RestoreBackupDefaults.backend
    assert RestoreBackupDefaults.backup_id == RestoreBackupDefaults.backup_id
    assert RestoreBackupDefaults.wait == RestoreBackupDefaults.wait
    assert RestoreBackupDefaults.include == RestoreBackupDefaults.include
    assert RestoreBackupDefaults.exclude == RestoreBackupDefaults.exclude


def test_update_collection_defaults(runner):
    # Create mock context with config
    ctx = click.Context(update_collection_cli)
    ctx.obj = {"config": MagicMock()}

    # Mock exists() to return False so create_collection succeeds
    mock_client = MagicMock()
    mock_client.collections.exists.side_effect = [True, True]

    # Mock collection object and replication factor
    mock_collection = MagicMock()
    mock_config = MagicMock()
    mock_config.replication_config.factor = 3
    mock_config.multi_tenancy_config.enabled = True
    mock_config.multi_tenancy_config.auto_tenant_creation = False
    mock_config.multi_tenancy_config.auto_tenant_activation = False
    mock_collection.config.get.return_value = mock_config
    mock_client.collections.get.return_value = mock_collection

    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(update_collection_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert UpdateCollectionDefaults.collection == UpdateCollectionDefaults.collection
    assert (
        UpdateCollectionDefaults.async_enabled == UpdateCollectionDefaults.async_enabled
    )
    assert (
        UpdateCollectionDefaults.vector_index == UpdateCollectionDefaults.vector_index
    )
    assert UpdateCollectionDefaults.description == UpdateCollectionDefaults.description
    assert (
        UpdateCollectionDefaults.training_limit
        == UpdateCollectionDefaults.training_limit
    )
    assert (
        UpdateCollectionDefaults.auto_tenant_creation
        == UpdateCollectionDefaults.auto_tenant_creation
    )
    assert (
        UpdateCollectionDefaults.auto_tenant_activation
        == UpdateCollectionDefaults.auto_tenant_activation
    )
    assert (
        UpdateCollectionDefaults.replication_deletion_strategy
        == UpdateCollectionDefaults.replication_deletion_strategy
    )


def test_update_tenants_defaults(runner):
    # Create mock context with config
    ctx = click.Context(update_tenants_cli)
    ctx.obj = {"config": MagicMock()}
    mock_client = MagicMock()

    # Mock exists() to return True so create_collection succeeds
    mock_client.collections.exists.return_value = True
    mock_client.get_meta.return_value = {"version": "1.25.0"}

    # Mock collection object
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Mock multi-tenancy config
    mock_config = MagicMock()
    mock_config.multi_tenancy_config.enabled = True
    mock_collection.config.get.return_value = mock_config

    # Mock tenants with side effect to return different values on subsequent calls
    mock_tenants = {
        f"{UpdateTenantsDefaults.tenant_suffix}{i}": MagicMock(
            activity_status=TenantActivityStatus.ACTIVE
        )
        for i in range(UpdateTenantsDefaults.number_tenants)
    }
    mock_collection.tenants.get.return_value = mock_tenants

    updated_tenants = {
        f"{UpdateTenantsDefaults.tenant_suffix}{i}": MagicMock(
            activity_status=TenantActivityStatus.ACTIVE
        )
        for i in range(UpdateTenantsDefaults.number_tenants)
    }
    mock_collection.tenants.get_by_names.return_value = updated_tenants
    ctx.obj["config"].get_client.return_value = mock_client

    result = runner.invoke(update_tenants_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert UpdateTenantsDefaults.collection == UpdateTenantsDefaults.collection
    assert UpdateTenantsDefaults.tenant_suffix == UpdateTenantsDefaults.tenant_suffix
    assert UpdateTenantsDefaults.number_tenants == UpdateTenantsDefaults.number_tenants
    assert UpdateTenantsDefaults.state == UpdateTenantsDefaults.state


def test_update_shards_defaults(runner):
    # Create mock context with config
    ctx = click.Context(update_shards_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(update_shards_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert UpdateShardsDefaults.collection == UpdateShardsDefaults.collection
    assert UpdateShardsDefaults.status == UpdateShardsDefaults.status
    assert UpdateShardsDefaults.shards == UpdateShardsDefaults.shards
    assert UpdateShardsDefaults.all == UpdateShardsDefaults.all


def test_update_data_defaults(runner):
    # Create mock context with config
    ctx = click.Context(update_data_cli)
    ctx.obj = {"config": MagicMock()}
    ctx.obj["config"].get_client = MagicMock()

    result = runner.invoke(update_data_cli, obj=ctx.obj)
    assert result.exit_code == 0, result.output
    assert UpdateDataDefaults.collection == UpdateDataDefaults.collection
    assert UpdateDataDefaults.limit == UpdateDataDefaults.limit
    assert UpdateDataDefaults.consistency_level == UpdateDataDefaults.consistency_level
    assert UpdateDataDefaults.randomize == UpdateDataDefaults.randomize
