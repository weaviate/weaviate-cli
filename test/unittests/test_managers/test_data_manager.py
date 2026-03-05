import pytest
from unittest.mock import MagicMock, patch
from weaviate_cli.managers.data_manager import DataManager
import weaviate.classes.config as wvc


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

    # Test data ingestion
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

    # Test data update
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

    # Test data deletion
    manager.delete_data(
        collection="TestCollection",
        limit=100,
    )

    mock_client.collections.get.assert_called_once_with("TestCollection")
