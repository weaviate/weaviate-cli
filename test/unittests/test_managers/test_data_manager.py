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


# ---------------------------------------------------------------------------
# _resolve_tenants_for_ingestion
# ---------------------------------------------------------------------------


def _make_col(name: str = "TestCollection") -> MagicMock:
    """Return a minimal mock Collection with configurable tenants.get()."""
    col = MagicMock()
    col.name = name
    return col


def _make_manager() -> DataManager:
    return DataManager(MagicMock())


class TestResolveTenants:
    """Unit tests for DataManager._resolve_tenants_for_ingestion."""

    # ------------------------------------------------------------------
    # Case 1: non-MT collection
    # ------------------------------------------------------------------

    def test_non_mt_no_options_returns_sentinel(self) -> None:
        col = _make_col()
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=False,
            auto_tenant_creation_enabled=False,
            tenant_suffix="Tenant",
            auto_tenants=0,
            tenants_list=None,
        )
        assert result == ["None"]

    def test_non_mt_with_tenants_list_raises(self) -> None:
        col = _make_col()
        manager = _make_manager()
        with pytest.raises(Exception, match="does not have multi-tenancy enabled"):
            manager._resolve_tenants_for_ingestion(
                col=col,
                mt_enabled=False,
                auto_tenant_creation_enabled=False,
                tenant_suffix="Tenant",
                auto_tenants=0,
                tenants_list=["Tenant-0"],
            )

    def test_non_mt_with_auto_tenants_raises(self) -> None:
        col = _make_col()
        manager = _make_manager()
        with pytest.raises(Exception, match="does not have multi-tenancy enabled"):
            manager._resolve_tenants_for_ingestion(
                col=col,
                mt_enabled=False,
                auto_tenant_creation_enabled=False,
                tenant_suffix="Tenant",
                auto_tenants=3,
                tenants_list=None,
            )

    # ------------------------------------------------------------------
    # Case 2: auto_tenants > 0
    # ------------------------------------------------------------------

    def test_auto_tenants_without_auto_creation_raises(self) -> None:
        col = _make_col()
        manager = _make_manager()
        with pytest.raises(Exception, match="Auto tenant creation is not enabled"):
            manager._resolve_tenants_for_ingestion(
                col=col,
                mt_enabled=True,
                auto_tenant_creation_enabled=False,
                tenant_suffix="Tenant",
                auto_tenants=3,
                tenants_list=None,
            )

    def test_auto_tenants_no_existing_generates_new(self) -> None:
        col = _make_col()
        col.tenants.get.return_value = {}
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=True,
            tenant_suffix="Tenant",
            auto_tenants=3,
            tenants_list=None,
        )
        assert result == ["Tenant-0", "Tenant-1", "Tenant-2"]

    def test_auto_tenants_continues_from_highest_index(self) -> None:
        col = _make_col()
        col.tenants.get.return_value = {
            "Tenant-0": MagicMock(),
            "Tenant-1": MagicMock(),
        }
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=True,
            tenant_suffix="Tenant",
            auto_tenants=4,
            tenants_list=None,
        )
        # 2 existing + 2 new starting at index 2
        assert result == ["Tenant-0", "Tenant-1", "Tenant-2", "Tenant-3"]

    def test_auto_tenants_enough_existing_returns_slice(self) -> None:
        col = _make_col()
        col.tenants.get.return_value = {
            "Tenant-0": MagicMock(),
            "Tenant-1": MagicMock(),
            "Tenant-2": MagicMock(),
        }
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=True,
            tenant_suffix="Tenant",
            auto_tenants=2,
            tenants_list=None,
        )
        assert result == ["Tenant-0", "Tenant-1"]

    def test_auto_tenants_non_numeric_suffix_skipped_in_indexing(self) -> None:
        """Tenants whose suffix is non-numeric are included but don't affect index."""
        col = _make_col()
        col.tenants.get.return_value = {
            "Tenant-abc": MagicMock(),  # non-numeric, skip for index
        }
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=True,
            tenant_suffix="Tenant",
            auto_tenants=2,
            tenants_list=None,
        )
        # existing_matching has "Tenant-abc", highest_index stays -1 → new ones start at 0
        assert "Tenant-abc" in result
        assert "Tenant-0" in result
        assert len(result) == 2

    # ------------------------------------------------------------------
    # Case 3: explicit tenants_list
    # ------------------------------------------------------------------

    def test_explicit_tenants_list_returned_as_is(self) -> None:
        col = _make_col()
        manager = _make_manager()
        tenants = ["Alpha", "Beta", "Gamma"]
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=False,
            tenant_suffix="Tenant",
            auto_tenants=0,
            tenants_list=tenants,
        )
        assert result is tenants

    # ------------------------------------------------------------------
    # Case 4: no options – fall back to existing tenants with suffix
    # ------------------------------------------------------------------

    def test_fallback_existing_tenants_with_suffix_returned(self) -> None:
        col = _make_col()
        col.tenants.get.return_value = {
            "Tenant-0": MagicMock(),
            "Tenant-1": MagicMock(),
            "Other-0": MagicMock(),  # different suffix, must be excluded
        }
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=False,
            tenant_suffix="Tenant",
            auto_tenants=0,
            tenants_list=None,
        )
        assert sorted(result) == ["Tenant-0", "Tenant-1"]

    def test_fallback_no_tenants_no_auto_creation_raises(self) -> None:
        col = _make_col()
        col.tenants.get.return_value = {}
        manager = _make_manager()
        with pytest.raises(Exception, match="No tenants present"):
            manager._resolve_tenants_for_ingestion(
                col=col,
                mt_enabled=True,
                auto_tenant_creation_enabled=False,
                tenant_suffix="Tenant",
                auto_tenants=0,
                tenants_list=None,
            )

    def test_fallback_no_tenants_with_auto_creation_returns_empty(self) -> None:
        """When auto_tenant_creation is on and no tenants exist, returns empty list."""
        col = _make_col()
        col.tenants.get.return_value = {}
        manager = _make_manager()
        result = manager._resolve_tenants_for_ingestion(
            col=col,
            mt_enabled=True,
            auto_tenant_creation_enabled=True,
            tenant_suffix="Tenant",
            auto_tenants=0,
            tenants_list=None,
        )
        assert result == []


# ---------------------------------------------------------------------------
# Alias resolution in data operations
# ---------------------------------------------------------------------------


class TestAliasResolution:
    """Unit tests for alias resolution in create/update/delete/query data."""

    def _setup_alias_mock(self, mock_client, alias_name="MyAlias"):
        """Set up a mock client where collection doesn't exist but alias does."""
        mock_collections = MagicMock()
        mock_collections.exists.return_value = False
        mock_client.collections = mock_collections

        mock_alias = MagicMock()
        mock_alias.list_all.return_value = {alias_name: MagicMock()}
        mock_client.alias = mock_alias

        mock_collection = MagicMock()
        mock_collection.config.get.return_value = MagicMock(
            multi_tenancy_config=MagicMock(
                enabled=False,
                auto_tenant_creation=False,
                auto_tenant_activation=False,
            )
        )
        mock_client.collections.get.return_value = mock_collection
        return mock_collection

    def test_create_data_with_alias(self, mock_client):
        self._setup_alias_mock(mock_client)
        manager = DataManager(mock_client)
        manager.create_data(collection="MyAlias", limit=10, randomize=True)
        mock_client.collections.get.assert_called_once_with("MyAlias")

    def test_update_data_with_alias(self, mock_client):
        self._setup_alias_mock(mock_client)
        manager = DataManager(mock_client)
        manager.update_data(collection="MyAlias", limit=10, randomize=True)
        mock_client.collections.get.assert_called_once_with("MyAlias")

    def test_delete_data_with_alias(self, mock_client):
        col = self._setup_alias_mock(mock_client)
        col.config.get.return_value.multi_tenancy_config.enabled = False
        # delete_data iterates objects, mock the iterator
        col.iterator.return_value = iter([])
        manager = DataManager(mock_client)
        manager.delete_data(collection="MyAlias", limit=10)
        mock_client.collections.get.assert_called_once_with("MyAlias")

    def test_query_data_with_alias(self, mock_client):
        col = self._setup_alias_mock(mock_client)
        col.config.get.return_value.multi_tenancy_config.enabled = False
        col.query.fetch_objects.return_value = MagicMock(objects=[])
        manager = DataManager(mock_client)
        manager.query_data(collection="MyAlias", search_type="fetch", limit=5)
        mock_client.collections.get.assert_called_once_with("MyAlias")

    def _setup_not_found_mock(self, mock_client):
        """Set up a mock client where neither collection nor alias exists."""
        mock_collections = MagicMock()
        mock_collections.exists.return_value = False
        mock_client.collections = mock_collections
        mock_alias = MagicMock()
        mock_alias.list_all.return_value = {}
        mock_client.alias = mock_alias

    def test_create_data_not_collection_not_alias_raises(self, mock_client):
        self._setup_not_found_mock(mock_client)
        manager = DataManager(mock_client)
        with pytest.raises(Exception, match="does not exist"):
            manager.create_data(collection="NonExistent", limit=10, randomize=True)

    def test_update_data_not_collection_not_alias_raises(self, mock_client):
        self._setup_not_found_mock(mock_client)
        manager = DataManager(mock_client)
        with pytest.raises(Exception, match="does not exist"):
            manager.update_data(collection="NonExistent", limit=10, randomize=True)

    def test_delete_data_not_collection_not_alias_raises(self, mock_client):
        self._setup_not_found_mock(mock_client)
        manager = DataManager(mock_client)
        with pytest.raises(Exception, match="does not exist"):
            manager.delete_data(collection="NonExistent", limit=10)

    def test_query_data_not_collection_not_alias_raises(self, mock_client):
        self._setup_not_found_mock(mock_client)
        manager = DataManager(mock_client)
        with pytest.raises(Exception, match="does not exist"):
            manager.query_data(collection="NonExistent", search_type="fetch", limit=5)

    def test_create_data_direct_collection_skips_alias_check(self, mock_client):
        """When collection exists directly, alias.list_all should not be called."""
        mock_collections = MagicMock()
        mock_collections.exists.return_value = True
        mock_client.collections = mock_collections
        mock_alias = MagicMock()
        mock_client.alias = mock_alias
        mock_collection = MagicMock()
        mock_collection.config.get.return_value = MagicMock(
            multi_tenancy_config=MagicMock(
                enabled=False,
                auto_tenant_creation=False,
                auto_tenant_activation=False,
            )
        )
        mock_client.collections.get.return_value = mock_collection
        manager = DataManager(mock_client)
        manager.create_data(collection="DirectCollection", limit=10, randomize=True)
        mock_client.alias.list_all.assert_not_called()


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
# create_data – parallel tenant processing
# ---------------------------------------------------------------------------


class TestCreateDataParallel:
    def _make_col(self, tenant_names):
        """MT collection with active tenant status ready for create_data."""
        col = _make_mt_col(tenant_names)
        tenant_status = MagicMock()
        tenant_status.activity_status = TenantActivityStatus.ACTIVE
        col.tenants.get_by_name.return_value = tenant_status
        return col

    def test_all_tenants_processed_in_parallel(self, mock_client):
        """All tenants are processed when parallel_workers > 1."""
        manager = DataManager(mock_client)
        col = self._make_col(["Tenant-0", "Tenant-1", "Tenant-2"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []
        lock = threading.Lock()

        def fake_ingest(collection, **kwargs):
            with lock:
                processed.append(collection)
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        assert len(processed) == 3

    def test_sequential_when_parallel_workers_is_1(self, mock_client):
        """When parallel_workers=1, all tenants are still processed (sequentially)."""
        manager = DataManager(mock_client)
        col = self._make_col(["Tenant-0", "Tenant-1", "Tenant-2"])
        _setup_mock_client_with_col(mock_client, col)

        processed = []

        def fake_ingest(collection, **kwargs):
            processed.append(collection)
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=1,
            )

        assert len(processed) == 3

    def test_parallel_errors_collected_and_raised(self, mock_client):
        """Errors from parallel tenant ingestion are collected and raised together."""
        manager = DataManager(mock_client)
        col = self._make_col(["Tenant-0", "Tenant-1"])
        _setup_mock_client_with_col(mock_client, col)

        def fake_ingest(collection, **kwargs):
            raise Exception("simulated ingest error")

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            with pytest.raises(
                Exception, match="Errors during parallel data ingestion"
            ):
                manager.create_data(
                    collection="TestCollection",
                    limit=5,
                    parallel_workers=4,
                )

    def test_non_mt_collection_unaffected(self, mock_client):
        """Non-MT collections are processed as a single 'None' tenant."""
        manager = DataManager(mock_client)
        col = _make_non_mt_col()
        _setup_mock_client_with_col(mock_client, col)

        processed = []

        def fake_ingest(collection, **kwargs):
            processed.append(collection)
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        # Single "None" pseudo-tenant processed
        assert len(processed) == 1

    def test_parallel_returns_tenant_scoped_collection(self, mock_client):
        """Parallel mode must return a tenant-scoped collection, not the base one.

        Regression test: parallel ingestion discarded the tenant-scoped collection
        returned by __ingest_data, causing callers to get back the base collection
        which fails with 'multi-tenancy enabled, but request was without tenant'.
        """
        manager = DataManager(mock_client)
        tenants = ["Tenant-0", "Tenant-1", "Tenant-2"]
        col = self._make_col(tenants)
        _setup_mock_client_with_col(mock_client, col)

        def fake_ingest(collection, **kwargs):
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            result = manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        # The returned collection must NOT be the base col (which has no tenant)
        assert (
            result is not col
        ), "Parallel mode returned the base collection instead of a tenant-scoped one"
        # It should be one of the tenant-scoped collections
        col.with_tenant.assert_called()

    def test_sequential_returns_tenant_scoped_collection(self, mock_client):
        """Sequential mode must also return a tenant-scoped collection."""
        manager = DataManager(mock_client)
        tenants = ["Tenant-0", "Tenant-1"]
        col = self._make_col(tenants)
        _setup_mock_client_with_col(mock_client, col)

        def fake_ingest(collection, **kwargs):
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            result = manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=1,
            )

        assert (
            result is not col
        ), "Sequential mode returned the base collection instead of a tenant-scoped one"

    def test_non_mt_returns_base_collection(self, mock_client):
        """Non-MT collections should return the base collection (no tenant context)."""
        manager = DataManager(mock_client)
        col = _make_non_mt_col()
        _setup_mock_client_with_col(mock_client, col)

        def fake_ingest(collection, **kwargs):
            return collection

        with patch.object(
            manager, "_DataManager__ingest_data", side_effect=fake_ingest
        ):
            result = manager.create_data(
                collection="TestCollection",
                limit=5,
                parallel_workers=4,
            )

        # For non-MT, the base collection IS the correct return value
        assert result is col


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

        # actual_workers = min(parallel_workers, len(tenants), concurrent_requests)
        #                = min(4, 2, 8) = 2
        # effective_concurrent = concurrent_requests // actual_workers = 8 // 2 = 4
        expected = max(1, 8 // min(4, 2, 8))
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
