import threading

import pytest
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.data_manager import DataManager
import weaviate.classes.config as wvc
from weaviate.collections.classes.tenants import TenantActivityStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_mock_client_with_col(mock_client, col):
    """Attach col to mock_client.collections with correct mock setup."""
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True
    mock_collections.get.return_value = col


def _make_mt_col(tenant_names):
    """Return a mock collection with MT enabled and the given tenant names."""
    col = MagicMock()
    col.name = "TestCollection"
    tenants_dict = {name: MagicMock() for name in tenant_names}
    col.config.get.return_value = MagicMock(
        multi_tenancy_config=MagicMock(
            enabled=True,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
        )
    )
    col.tenants.get.return_value = tenants_dict
    # with_tenant returns a fresh mock per tenant
    col.with_tenant.side_effect = lambda name: MagicMock(name=f"col__{name}")
    col.__len__ = MagicMock(return_value=0)
    return col


def _make_non_mt_col():
    """Return a mock collection with MT disabled."""
    col = MagicMock()
    col.name = "TestCollection"
    col.config.get.return_value = MagicMock(
        multi_tenancy_config=MagicMock(
            enabled=False,
            auto_tenant_creation=False,
            auto_tenant_activation=False,
        )
    )
    col.__len__ = MagicMock(return_value=0)
    return col


# ---------------------------------------------------------------------------
# Existing smoke tests (kept for backwards compat)
# ---------------------------------------------------------------------------


def test_ingest_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    mock_collection.config.get.return_value = MagicMock(
        multi_tenancy_config=MagicMock(auto_tenant_creation=True)
    )

    manager.create_data(
        collection="TestCollection",
        limit=100,
        randomize=True,
        auto_tenants=10,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")


def test_update_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    manager.update_data(
        collection="TestCollection",
        limit=100,
        randomize=True,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")


def test_delete_data(mock_client):
    manager = DataManager(mock_client)
    mock_collections = MagicMock()
    mock_client.collections = mock_collections
    mock_collections.exists.return_value = True

    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    manager.delete_data(
        collection="TestCollection",
        limit=100,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")


# ---------------------------------------------------------------------------
# update_data – parallel tenant processing
# ---------------------------------------------------------------------------


class TestUpdateDataParallel:
    def test_all_tenants_processed_in_parallel(self, mock_client):
        """All tenants are processed when parallel_workers > 1."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1", "Tenant-2"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []
        lock = threading.Lock()

        def fake_update(collection, *args, **kwargs):
            with lock:
                processed.append(collection)
            return 5

        with patch.object(
            manager, "_DataManager__update_data", side_effect=fake_update
        ):
            manager.update_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        assert len(processed) == 3

    def test_sequential_when_parallel_workers_is_1(self, mock_client):
        """When parallel_workers=1, all tenants are still processed (sequentially)."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1", "Tenant-2"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []

        def fake_update(collection, *args, **kwargs):
            processed.append(collection)
            return 3

        with patch.object(
            manager, "_DataManager__update_data", side_effect=fake_update
        ):
            manager.update_data(
                collection="TestCollection",
                limit=3,
                parallel_workers=1,
            )

        assert len(processed) == 3

    def test_parallel_errors_collected_and_raised(self, mock_client):
        """Errors from parallel tenant updates are collected and raised together."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1"])
        _setup_mock_client_with_col(mock_client, col)

        def fake_update(collection, *args, **kwargs):
            raise Exception("simulated update error")

        with patch.object(
            manager, "_DataManager__update_data", side_effect=fake_update
        ):
            with pytest.raises(Exception, match="Errors during parallel data update"):
                manager.update_data(
                    collection="TestCollection",
                    limit=5,
                    parallel_workers=4,
                )

    def test_non_mt_collection_unaffected(self, mock_client):
        """Non-MT collections are processed the same regardless of parallel_workers."""
        manager = DataManager(mock_client)
        col = _make_non_mt_col()
        _setup_mock_client_with_col(mock_client, col)
        # Simulate MT-disabled exception from tenants.get()
        col.tenants.get.side_effect = Exception("multi-tenancy is not enabled")

        processed = []

        def fake_update(collection, *args, **kwargs):
            processed.append(collection)
            return 10

        with patch.object(
            manager, "_DataManager__update_data", side_effect=fake_update
        ):
            manager.update_data(
                collection="TestCollection",
                limit=10,
                parallel_workers=4,
            )

        # Single "None" pseudo-tenant processed
        assert len(processed) == 1


# ---------------------------------------------------------------------------
# delete_data – parallel tenant processing
# ---------------------------------------------------------------------------


class TestDeleteDataParallel:
    def test_all_tenants_processed_in_parallel(self, mock_client):
        """All tenants are processed when parallel_workers > 1."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1", "Tenant-2"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []
        lock = threading.Lock()

        def fake_delete(collection, *args, **kwargs):
            with lock:
                processed.append(collection)
            return 5

        with patch.object(
            manager, "_DataManager__delete_data", side_effect=fake_delete
        ):
            manager.delete_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        assert len(processed) == 3

    def test_sequential_when_parallel_workers_is_1(self, mock_client):
        """When parallel_workers=1, all tenants are still processed (sequentially)."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []

        def fake_delete(collection, *args, **kwargs):
            processed.append(collection)
            return 3

        with patch.object(
            manager, "_DataManager__delete_data", side_effect=fake_delete
        ):
            manager.delete_data(
                collection="TestCollection",
                limit=3,
                parallel_workers=1,
            )

        assert len(processed) == 2

    def test_parallel_errors_collected_and_raised(self, mock_client):
        """Errors from parallel tenant deletions are collected and raised together."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1"])
        _setup_mock_client_with_col(mock_client, col)

        def fake_delete(collection, *args, **kwargs):
            raise Exception("simulated delete error")

        with patch.object(
            manager, "_DataManager__delete_data", side_effect=fake_delete
        ):
            with pytest.raises(Exception, match="Errors during parallel data deletion"):
                manager.delete_data(
                    collection="TestCollection",
                    limit=5,
                    parallel_workers=4,
                )

    def test_specific_tenants_list_processed_in_parallel(self, mock_client):
        """When tenants_list is provided, only those tenants are processed."""
        manager = DataManager(mock_client)
        # Collection has 5 tenants but we only target 2
        col = _make_mt_col([f"Tenant-{i}" for i in range(5)])
        _setup_mock_client_with_col(mock_client, col)

        processed = []
        lock = threading.Lock()

        def fake_delete(collection, *args, **kwargs):
            with lock:
                processed.append(collection)
            return 1

        with patch.object(
            manager, "_DataManager__delete_data", side_effect=fake_delete
        ):
            manager.delete_data(
                collection="TestCollection",
                limit=1,
                tenants_list=["Tenant-0", "Tenant-2"],
                parallel_workers=4,
            )

        assert len(processed) == 2

    def test_non_mt_collection_unaffected(self, mock_client):
        """Non-MT collections are processed without parallelism."""
        manager = DataManager(mock_client)
        col = _make_non_mt_col()
        _setup_mock_client_with_col(mock_client, col)

        processed = []

        def fake_delete(collection, *args, **kwargs):
            processed.append(collection)
            return 5

        with patch.object(
            manager, "_DataManager__delete_data", side_effect=fake_delete
        ):
            manager.delete_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        assert len(processed) == 1


# ---------------------------------------------------------------------------
# create_data – concurrent_requests scaling with parallel_workers
# ---------------------------------------------------------------------------


class TestCreateDataConcurrentRequestsScaling:
    def test_concurrent_requests_reduced_for_parallel_tenants(self, mock_client):
        """When parallel_workers > 1 with multiple tenants, concurrent_requests
        per tenant is divided to keep total connections bounded."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0", "Tenant-1"])
        col.tenants.exists.return_value = True
        tenant_status = MagicMock()
        tenant_status.activity_status = TenantActivityStatus.ACTIVE
        col.tenants.get_by_name.return_value = tenant_status
        tenant_col = MagicMock()
        tenant_col.__len__ = MagicMock(return_value=0)
        col.with_tenant.return_value = tenant_col
        _setup_mock_client_with_col(mock_client, col)

        captured_concurrent = []
        lock = threading.Lock()

        def fake_ingest(collection, *, concurrent_requests, **kwargs):
            with lock:
                captured_concurrent.append(concurrent_requests)
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            manager.create_data(
                collection="TestCollection",
                limit=10,
                randomize=True,
                tenant_suffix="Tenant",
                concurrent_requests=8,
                parallel_workers=4,
            )

        # Each tenant should get concurrent_requests // parallel_workers = 2
        expected = max(1, 8 // 4)
        assert all(c == expected for c in captured_concurrent)
        assert len(captured_concurrent) == 2

    def test_concurrent_requests_unchanged_for_single_tenant(self, mock_client):
        """With a single tenant, concurrent_requests is not reduced."""
        manager = DataManager(mock_client)
        col = _make_mt_col(["Tenant-0"])
        col.tenants.exists.return_value = True
        tenant_status = MagicMock()
        tenant_status.activity_status = TenantActivityStatus.ACTIVE
        col.tenants.get_by_name.return_value = tenant_status
        tenant_col = MagicMock()
        tenant_col.__len__ = MagicMock(return_value=0)
        col.with_tenant.return_value = tenant_col
        _setup_mock_client_with_col(mock_client, col)

        captured_concurrent = []

        def fake_ingest(collection, *, concurrent_requests, **kwargs):
            captured_concurrent.append(concurrent_requests)
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            manager.create_data(
                collection="TestCollection",
                limit=10,
                randomize=True,
                tenant_suffix="Tenant",
                concurrent_requests=8,
                parallel_workers=4,
            )

        # Single tenant: no reduction
        assert captured_concurrent == [8]
