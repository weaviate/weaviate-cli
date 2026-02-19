import json
import pytest
from unittest.mock import MagicMock

from weaviate.collections.classes.tenants import TenantActivityStatus

from weaviate_cli.managers.tenant_manager import TenantManager
from weaviate_cli.defaults import (
    CreateTenantsDefaults,
    DeleteTenantsDefaults,
    GetTenantsDefaults,
    UpdateTenantsDefaults,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WEAVIATE_VERSION_GTE_1_25 = "1.26.0"
WEAVIATE_VERSION_LT_1_25 = "1.24.0"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_client() -> MagicMock:
    """
    Return a plain MagicMock for the Weaviate client.

    We intentionally do NOT use spec=weaviate.WeaviateClient here because
    the spec only exposes a handful of top-level methods and does not expose
    `collections` (which is a property on the real object). Using a plain
    MagicMock lets us configure .collections.* freely, matching how
    TenantManager actually uses the client.
    """
    return MagicMock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tenant(activity_status: TenantActivityStatus) -> MagicMock:
    """Return a mock tenant object with the given activity_status."""
    t = MagicMock()
    t.activity_status = activity_status
    return t


def _make_manager_and_collection(
    mock_client: MagicMock,
    *,
    collection_exists: bool = True,
    mt_enabled: bool = True,
    version: str = WEAVIATE_VERSION_GTE_1_25,
    collection_name: str = "TestCollection",
) -> tuple[TenantManager, MagicMock]:
    """
    Wire up mock_client and return (TenantManager, mock_collection).

    The caller can further configure mock_collection.tenants.* to suit each
    test scenario.
    """
    mock_client.collections.exists.return_value = collection_exists
    mock_client.get_meta.return_value = {"version": version}

    mock_collection = MagicMock()
    mock_collection.name = collection_name
    mock_collection.config.get.return_value = MagicMock(
        multi_tenancy_config=MagicMock(enabled=mt_enabled)
    )
    mock_client.collections.get.return_value = mock_collection

    return TenantManager(mock_client), mock_collection


# ---------------------------------------------------------------------------
# create_tenants
# ---------------------------------------------------------------------------


class TestCreateTenants:
    def test_raises_when_collection_does_not_exist(
        self, mock_client: MagicMock
    ) -> None:
        """create_tenants raises when the target collection is missing."""
        manager, _ = _make_manager_and_collection(mock_client, collection_exists=False)

        with pytest.raises(Exception, match="does not exist"):
            manager.create_tenants(collection="Missing")

    def test_raises_when_multi_tenancy_not_enabled(
        self, mock_client: MagicMock
    ) -> None:
        """create_tenants raises when MT is disabled on the collection."""
        manager, _ = _make_manager_and_collection(mock_client, mt_enabled=False)

        with pytest.raises(Exception, match="does not have multi-tenancy enabled"):
            manager.create_tenants(collection="TestCollection")

    def test_happy_path_creates_n_tenants_from_zero_text_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        With no existing tenants, creates N tenants named Tenant-0 … Tenant-N-1
        and emits the text success message.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        # No existing tenants
        mock_collection.tenants.get.return_value = {}

        tenant_suffix = "Tenant"
        number_tenants = 3
        expected_names = [f"{tenant_suffix}-{i}" for i in range(number_tenants)]

        # get_by_names returns a dict with the right statuses
        created = {
            name: _make_tenant(TenantActivityStatus.ACTIVE) for name in expected_names
        }
        mock_collection.tenants.get_by_names.return_value = created

        manager.create_tenants(
            collection="TestCollection",
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
            state="active",
            json_output=False,
        )

        out = capsys.readouterr().out
        assert str(number_tenants) in out
        assert "tenants added" in out

    def test_happy_path_creates_n_tenants_from_zero_json_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """create_tenants emits valid JSON with json_output=True."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get.return_value = {}

        number_tenants = 2
        tenant_suffix = "Tenant"
        expected_names = [f"{tenant_suffix}-{i}" for i in range(number_tenants)]
        created = {
            name: _make_tenant(TenantActivityStatus.ACTIVE) for name in expected_names
        }
        mock_collection.tenants.get_by_names.return_value = created

        manager.create_tenants(
            collection="TestCollection",
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
            state="active",
            json_output=True,
        )

        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["status"] == "success"
        assert data["tenants_created"] == number_tenants
        assert str(number_tenants) in data["message"]

    def test_continues_numbering_from_highest_plus_one(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        When existing tenants are present that follow the pattern, new tenants
        continue numbering from highest_index + 1.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        # Existing tenants: Tenant-0 and Tenant-1 (highest index = 1)
        existing = {
            "Tenant-0": _make_tenant(TenantActivityStatus.ACTIVE),
            "Tenant-1": _make_tenant(TenantActivityStatus.ACTIVE),
        }
        mock_collection.tenants.get.return_value = existing

        number_new = 2
        # Expected new names start from index 2
        expected_new_names = ["Tenant-2", "Tenant-3"]
        created = {
            name: _make_tenant(TenantActivityStatus.ACTIVE)
            for name in expected_new_names
        }
        mock_collection.tenants.get_by_names.return_value = created

        manager.create_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=number_new,
            state="active",
            json_output=False,
        )

        # Verify get_by_names was called with the new names only
        mock_collection.tenants.get_by_names.assert_called_once_with(expected_new_names)
        out = capsys.readouterr().out
        assert str(number_new) in out

    def test_raises_when_existing_tenant_does_not_match_suffix_pattern(
        self, mock_client: MagicMock
    ) -> None:
        """
        When an existing tenant name starts with the suffix but the index part
        is not a valid integer, create_tenants raises.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        # Existing tenant whose index portion is non-numeric
        mock_collection.tenants.get.return_value = {
            "Tenant-abc": _make_tenant(TenantActivityStatus.ACTIVE),
        }

        with pytest.raises(Exception, match="does not follow the expected pattern"):
            manager.create_tenants(
                collection="TestCollection",
                tenant_suffix="Tenant",
                number_tenants=1,
                state="active",
            )

    def test_raises_when_existing_tenant_uses_different_suffix(
        self, mock_client: MagicMock
    ) -> None:
        """
        When an existing tenant does not start with the provided suffix,
        create_tenants raises.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get.return_value = {
            "OtherSuffix-0": _make_tenant(TenantActivityStatus.ACTIVE),
        }

        with pytest.raises(Exception, match="does not use the provided tenant_suffix"):
            manager.create_tenants(
                collection="TestCollection",
                tenant_suffix="Tenant",
                number_tenants=1,
                state="active",
            )

    def test_uses_batching_when_tenant_batch_size_provided(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """When tenant_batch_size is given, tenants.create is called in batches."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get.return_value = {}

        number_tenants = 5
        batch_size = 2
        expected_names = [f"Tenant-{i}" for i in range(number_tenants)]
        created = {
            name: _make_tenant(TenantActivityStatus.ACTIVE) for name in expected_names
        }
        mock_collection.tenants.get_by_names.return_value = created

        manager.create_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=number_tenants,
            tenant_batch_size=batch_size,
            state="active",
            json_output=False,
        )

        # 5 tenants with batch_size=2 → 3 calls: [0,1], [2,3], [4]
        assert mock_collection.tenants.create.call_count == 3

    def test_raises_when_not_all_tenants_were_created(
        self, mock_client: MagicMock
    ) -> None:
        """
        If get_by_names returns fewer tenants than requested,
        create_tenants raises a descriptive error.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get.return_value = {}
        # Only 1 of 2 tenants came back from the server
        mock_collection.tenants.get_by_names.return_value = {
            "Tenant-0": _make_tenant(TenantActivityStatus.ACTIVE),
        }

        with pytest.raises(Exception, match="Not all requested tenants were created"):
            manager.create_tenants(
                collection="TestCollection",
                tenant_suffix="Tenant",
                number_tenants=2,
                state="active",
            )

    def test_uses_get_fallback_for_version_lt_1_25(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        For Weaviate < 1.25.0, verification falls back to tenants.get() instead
        of tenants.get_by_names().
        """
        manager, mock_collection = _make_manager_and_collection(
            mock_client, version=WEAVIATE_VERSION_LT_1_25
        )

        expected_names = ["Tenant-0", "Tenant-1"]
        all_tenants = {
            name: _make_tenant(TenantActivityStatus.ACTIVE) for name in expected_names
        }
        # First call (existing check) returns empty; second call (verify) returns all
        mock_collection.tenants.get.side_effect = [{}, all_tenants]

        manager.create_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=2,
            state="active",
            json_output=False,
        )

        # get_by_names must NOT have been called for old versions
        mock_collection.tenants.get_by_names.assert_not_called()
        out = capsys.readouterr().out
        assert "2" in out


# ---------------------------------------------------------------------------
# delete_tenants
# ---------------------------------------------------------------------------


class TestDeleteTenants:
    def test_raises_when_collection_does_not_exist(
        self, mock_client: MagicMock
    ) -> None:
        """delete_tenants raises when the target collection is missing."""
        manager, _ = _make_manager_and_collection(mock_client, collection_exists=False)

        with pytest.raises(Exception, match="does not exist"):
            manager.delete_tenants(collection="Missing")

    def test_raises_when_multi_tenancy_not_enabled(
        self, mock_client: MagicMock
    ) -> None:
        """delete_tenants raises when MT is disabled."""
        manager, _ = _make_manager_and_collection(mock_client, mt_enabled=False)

        with pytest.raises(Exception, match="does not have multi-tenancy enabled"):
            manager.delete_tenants(collection="TestCollection")

    def test_raises_when_no_tenants_present(self, mock_client: MagicMock) -> None:
        """delete_tenants raises when the collection has no tenants."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get.return_value = {}

        with pytest.raises(Exception, match="No tenants present"):
            manager.delete_tenants(
                collection="TestCollection",
                tenant_suffix="Tenant",
                number_tenants=1,
            )

    def test_happy_path_deletes_tenants_text_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """delete_tenants deletes the requested number and prints text output."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        tenant_name = "Tenant-0"
        mock_tenant = _make_tenant(TenantActivityStatus.ACTIVE)

        # First call: list tenants to decide what to delete
        # Second call: verify remaining count after deletion
        mock_collection.tenants.get.side_effect = [
            {tenant_name: mock_tenant},  # initial listing
            {},  # after deletion (0 remaining)
        ]

        manager.delete_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            json_output=False,
        )

        mock_collection.tenants.remove.assert_called_once_with(mock_tenant)
        out = capsys.readouterr().out
        assert "1" in out
        assert "deleted" in out

    def test_happy_path_deletes_tenants_json_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """delete_tenants emits valid JSON with json_output=True."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        mock_collection.tenants.get.side_effect = [
            {"Tenant-0": mock_tenant},
            {},
        ]

        manager.delete_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            json_output=True,
        )

        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["status"] == "success"
        assert data["tenants_deleted"] == 1
        assert "deleted" in data["message"]

    def test_deletes_only_up_to_number_tenants(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        When there are more tenants than number_tenants, only the first
        number_tenants are deleted.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        tenants = {
            f"Tenant-{i}": _make_tenant(TenantActivityStatus.ACTIVE) for i in range(5)
        }
        mock_collection.tenants.get.side_effect = [
            tenants,  # initial listing
            dict(list(tenants.items())[2:]),  # 3 remaining after deleting 2
        ]

        manager.delete_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=2,
            json_output=False,
        )

        assert mock_collection.tenants.remove.call_count == 2

    def test_deletes_using_tenants_list(self, mock_client: MagicMock, capsys) -> None:
        """When tenants_list is provided, get_by_names is used."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        specific_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        # tenants.get() is called for the suffix-filter even when tenants_list given
        mock_collection.tenants.get.side_effect = [
            {"Tenant-5": specific_tenant},  # suffix-filter listing
            {},  # post-delete verification
        ]
        mock_collection.tenants.get_by_names.return_value = {
            "Tenant-5": specific_tenant
        }

        manager.delete_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            tenants_list=["Tenant-5"],
            number_tenants=1,
            json_output=False,
        )

        mock_collection.tenants.get_by_names.assert_called_once_with(["Tenant-5"])
        mock_collection.tenants.remove.assert_called_once_with(specific_tenant)

    def test_delete_all_with_wildcard_suffix(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """When tenant_suffix='*', all tenants in the collection are deleted."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        tenants = {
            "Alpha-0": _make_tenant(TenantActivityStatus.ACTIVE),
            "Beta-0": _make_tenant(TenantActivityStatus.INACTIVE),
        }
        mock_collection.tenants.get.side_effect = [
            tenants,  # initial listing (wildcard uses all)
            {},  # post-delete verification
        ]

        manager.delete_tenants(
            collection="TestCollection",
            tenant_suffix="*",
            number_tenants=2,
            json_output=False,
        )

        assert mock_collection.tenants.remove.call_count == 2


# ---------------------------------------------------------------------------
# get_tenants
# ---------------------------------------------------------------------------


class TestGetTenants:
    def test_happy_path_all_tenants_json_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """get_tenants returns all tenants and emits valid JSON."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        active_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        inactive_tenant = _make_tenant(TenantActivityStatus.INACTIVE)

        tenants = {
            "Tenant-0": active_tenant,
            "Tenant-1": inactive_tenant,
        }
        mock_collection.tenants.get.return_value = tenants

        result = manager.get_tenants(
            collection="TestCollection",
            tenant_id=None,
            verbose=False,
            json_output=True,
        )

        assert result == tenants
        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["summary"]["total"] == 2
        assert data["summary"]["active"] == 1
        assert data["summary"]["inactive"] == 1
        assert len(data["tenants"]) == 2

    def test_happy_path_all_tenants_verbose_text_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """get_tenants with verbose=True prints per-tenant rows."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        t = _make_tenant(TenantActivityStatus.ACTIVE)
        mock_collection.tenants.get.return_value = {"Tenant-0": t}

        manager.get_tenants(
            collection="TestCollection",
            tenant_id=None,
            verbose=True,
            json_output=False,
        )

        out = capsys.readouterr().out
        assert "Tenant-0" in out
        assert "ACTIVE" in out

    def test_happy_path_all_tenants_summary_text_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """get_tenants with verbose=False prints the summary row."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        active_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        inactive_tenant = _make_tenant(TenantActivityStatus.INACTIVE)
        offloaded_tenant = _make_tenant(TenantActivityStatus.OFFLOADED)
        mock_collection.tenants.get.return_value = {
            "Tenant-0": active_tenant,
            "Tenant-1": inactive_tenant,
            "Tenant-2": offloaded_tenant,
        }

        manager.get_tenants(
            collection="TestCollection",
            tenant_id=None,
            verbose=False,
            json_output=False,
        )

        out = capsys.readouterr().out
        # Summary header and data row should both be present
        assert "Number Tenants" in out
        assert "3" in out

    def test_with_tenant_id_found(self, mock_client: MagicMock, capsys) -> None:
        """get_tenants returns the single tenant when tenant_id is given and found."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        t = _make_tenant(TenantActivityStatus.ACTIVE)
        mock_collection.tenants.get_by_names.return_value = {"Tenant-5": t}

        result = manager.get_tenants(
            collection="TestCollection",
            tenant_id="Tenant-5",
            verbose=True,
            json_output=False,
        )

        mock_collection.tenants.get_by_names.assert_called_once_with(["Tenant-5"])
        assert "Tenant-5" in result

    def test_with_tenant_id_not_found_raises(self, mock_client: MagicMock) -> None:
        """get_tenants raises when tenant_id is given but not found."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        mock_collection.tenants.get_by_names.return_value = {}

        with pytest.raises(Exception, match="not found"):
            manager.get_tenants(
                collection="TestCollection",
                tenant_id="NonExistentTenant",
                verbose=False,
                json_output=False,
            )

    def test_returns_tenant_dict(self, mock_client: MagicMock, capsys) -> None:
        """get_tenants always returns the dict of tenants."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        tenants = {"Tenant-0": _make_tenant(TenantActivityStatus.ACTIVE)}
        mock_collection.tenants.get.return_value = tenants

        result = manager.get_tenants(
            collection="TestCollection",
            json_output=False,
        )

        assert result is tenants


# ---------------------------------------------------------------------------
# update_tenants
# ---------------------------------------------------------------------------


class TestUpdateTenants:
    def test_raises_when_collection_does_not_exist(
        self, mock_client: MagicMock
    ) -> None:
        """update_tenants raises when the target collection is missing."""
        manager, _ = _make_manager_and_collection(mock_client, collection_exists=False)

        with pytest.raises(Exception, match="does not exist"):
            manager.update_tenants(collection="Missing")

    def test_raises_when_multi_tenancy_not_enabled(
        self, mock_client: MagicMock
    ) -> None:
        """update_tenants raises when MT is disabled."""
        manager, _ = _make_manager_and_collection(mock_client, mt_enabled=False)

        with pytest.raises(Exception, match="does not have multi-tenancy enabled"):
            manager.update_tenants(collection="TestCollection")

    def test_raises_when_not_enough_tenants_with_suffix(
        self, mock_client: MagicMock
    ) -> None:
        """
        update_tenants raises when fewer tenants than number_tenants have the
        requested suffix.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        # Only 1 tenant with the suffix but we request 5
        mock_collection.tenants.get.return_value = {
            "Tenant-0": _make_tenant(TenantActivityStatus.INACTIVE),
        }

        with pytest.raises(Exception, match="Not enough tenants present"):
            manager.update_tenants(
                collection="TestCollection",
                tenant_suffix="Tenant",
                number_tenants=5,
                state="active",
            )

    def test_happy_path_updates_state_json_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """update_tenants updates tenant state and emits valid JSON."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        inactive_tenant = _make_tenant(TenantActivityStatus.INACTIVE)
        inactive_tenant.name = "Tenant-0"

        # tenants.get() used for suffix-filter
        mock_collection.tenants.get.return_value = {
            "Tenant-0": inactive_tenant,
        }

        # After update, get_by_names returns the tenant with ACTIVE status
        updated_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        updated_tenant.name = "Tenant-0"
        mock_collection.tenants.get_by_names.return_value = {
            "Tenant-0": updated_tenant,
        }

        manager.update_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            state="active",
            tenants=None,
            json_output=True,
        )

        out = capsys.readouterr().out
        data = json.loads(out)
        assert data["status"] == "success"
        assert data["tenants_updated"] == 1

    def test_happy_path_updates_state_text_output(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """update_tenants emits text output when json_output=False."""
        manager, mock_collection = _make_manager_and_collection(mock_client)

        inactive_tenant = _make_tenant(TenantActivityStatus.INACTIVE)
        inactive_tenant.name = "Tenant-0"

        mock_collection.tenants.get.return_value = {
            "Tenant-0": inactive_tenant,
        }

        updated_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        updated_tenant.name = "Tenant-0"
        mock_collection.tenants.get_by_names.return_value = {
            "Tenant-0": updated_tenant,
        }

        manager.update_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            state="active",
            tenants=None,
            json_output=False,
        )

        out = capsys.readouterr().out
        assert "1" in out
        assert "updated" in out

    def test_update_via_tenants_argument(self, mock_client: MagicMock, capsys) -> None:
        """
        When the tenants argument (comma-separated string) is given,
        get_by_names is used to fetch those specific tenants.
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        cold_tenant = _make_tenant(TenantActivityStatus.INACTIVE)
        cold_tenant.name = "Tenant-3"

        mock_collection.tenants.get_by_names.side_effect = [
            {"Tenant-3": cold_tenant},  # fetch for update
            {"Tenant-3": cold_tenant},  # post-update verification
        ]

        manager.update_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            state="cold",
            tenants="Tenant-3",
            json_output=False,
        )

        # tenants.get() should NOT have been called for the suffix-filter path
        mock_collection.tenants.get.assert_not_called()
        out = capsys.readouterr().out
        assert "updated" in out

    def test_uses_get_fallback_for_version_lt_1_25(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        For Weaviate < 1.25.0, verification after update falls back to
        tenants.get() filtered by name instead of get_by_names().
        """
        manager, mock_collection = _make_manager_and_collection(
            mock_client, version=WEAVIATE_VERSION_LT_1_25
        )

        existing_tenant = _make_tenant(TenantActivityStatus.INACTIVE)
        existing_tenant.name = "Tenant-0"

        updated_tenant = _make_tenant(TenantActivityStatus.ACTIVE)
        updated_tenant.name = "Tenant-0"

        # First call → suffix-filter; second call → post-update verify
        mock_collection.tenants.get.side_effect = [
            {"Tenant-0": existing_tenant},
            {"Tenant-0": updated_tenant},
        ]

        manager.update_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            state="active",
            tenants=None,
            json_output=False,
        )

        # get_by_names must NOT have been called for old versions
        mock_collection.tenants.get_by_names.assert_not_called()
        out = capsys.readouterr().out
        assert "updated" in out

    def test_skips_update_call_when_tenant_already_in_desired_state(
        self, mock_client: MagicMock, capsys
    ) -> None:
        """
        When a tenant already has the target activity_status, tenants.update
        is called with the tenant as-is (no new Tenant object).
        """
        manager, mock_collection = _make_manager_and_collection(mock_client)

        already_active = _make_tenant(TenantActivityStatus.ACTIVE)
        already_active.name = "Tenant-0"

        mock_collection.tenants.get.return_value = {
            "Tenant-0": already_active,
        }
        mock_collection.tenants.get_by_names.return_value = {
            "Tenant-0": already_active,
        }

        manager.update_tenants(
            collection="TestCollection",
            tenant_suffix="Tenant",
            number_tenants=1,
            state="active",
            tenants=None,
            json_output=False,
        )

        # The existing tenant object should be passed directly (no new Tenant)
        mock_collection.tenants.update.assert_called_once_with(already_active)
