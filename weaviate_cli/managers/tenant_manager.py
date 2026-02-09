from typing import Optional, List
import click
import semver

from weaviate import WeaviateClient
from weaviate.collections.classes.tenants import TenantActivityStatus, Tenant
from weaviate_cli.defaults import (
    CreateTenantsDefaults,
    UpdateTenantsDefaults,
    DeleteTenantsDefaults,
    GetTenantsDefaults,
)


class TenantManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def create_tenants(
        self,
        collection: str = CreateTenantsDefaults.collection,
        tenant_suffix: str = CreateTenantsDefaults.tenant_suffix,
        number_tenants: int = CreateTenantsDefaults.number_tenants,
        tenant_batch_size: Optional[int] = CreateTenantsDefaults.tenant_batch_size,
        state: str = CreateTenantsDefaults.state,
        tenants_list: Optional[List[str]] = None,
    ) -> None:
        """
        Create tenants for a given collection in Weaviate.

        Args:
            collection (str): The name of the collection to add tenants to.
            tenant_suffix (str): The prefix for auto-generated tenant names.
            number_tenants (int): The number of tenants to auto-generate (ignored if tenants_list provided).
            tenant_batch_size (int): Batch size for tenant creation requests.
            state (str): The activity status of the tenants to be created.
            tenants_list (List[str]): Explicit list of tenant names to create.

        Raises:
            Exception: If the collection does not exist or multi-tenancy is not enabled.
        """

        if not self.client.collections.exists(collection):
            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class>"
            )

        version = semver.Version.parse(self.client.get_meta()["version"])
        collection = self.client.collections.get(collection)

        if not collection.config.get().multi_tenancy_config.enabled:
            raise Exception(
                f"Collection '{collection.name}' does not have multi-tenancy enabled. Recreate or modify the class with: <create class>"
            )

        tenant_state_map = {
            "hot": TenantActivityStatus.HOT,
            "active": TenantActivityStatus.ACTIVE,
            "cold": TenantActivityStatus.COLD,
            "inactive": TenantActivityStatus.INACTIVE,
            "frozen": TenantActivityStatus.FROZEN,
            "offloaded": TenantActivityStatus.OFFLOADED,
        }

        # Resolve tenant names to create
        new_tenant_names = self._resolve_tenant_names_to_create(
            collection=collection,
            tenant_suffix=tenant_suffix,
            number_tenants=number_tenants,
            tenants_list=tenants_list,
        )

        # Filter out already existing tenants
        existing_tenants = collection.tenants.get()
        new_tenant_names = [
            name for name in new_tenant_names if name not in existing_tenants
        ]

        # Create the new tenants
        tenants = [
            Tenant(name=name, activity_status=tenant_state_map[state])
            for name in new_tenant_names
        ]

        if not tenants:
            click.echo(f"No new tenants to create for collection '{collection.name}'")
            return

        # Create tenants (in batches if specified)
        if tenant_batch_size:
            for i in range(0, len(tenants), tenant_batch_size):
                batch = tenants[i : i + tenant_batch_size]
                collection.tenants.create(batch)
        else:
            collection.tenants.create(tenants)

        # Verify creation
        self._verify_tenant_creation(
            collection=collection,
            version=version,
            new_tenant_names=new_tenant_names,
            expected_status=tenant_state_map[state],
        )

        click.echo(
            f"{len(new_tenant_names)} tenants added with status '{tenant_state_map[state]}' for collection '{collection.name}'"
        )

    def _resolve_tenant_names_to_create(
        self,
        collection,
        tenant_suffix: str,
        number_tenants: int,
        tenants_list: Optional[List[str]],
    ) -> List[str]:
        """
        Determine which tenant names to create.

        Cases:
        1. Explicit tenants_list provided: use those names directly
        2. Auto-generate with suffix: find highest existing index for this suffix, continue from there
        """
        # Case 1: Explicit tenant list
        if tenants_list is not None:
            return tenants_list

        # Case 2: Auto-generate with suffix pattern
        # Only look at existing tenants matching this suffix (ignore others)
        existing_tenants = collection.tenants.get()
        highest_index = -1

        for tenant_name in existing_tenants.keys():
            if tenant_name.startswith(f"{tenant_suffix}-"):
                try:
                    index = int(tenant_name[len(f"{tenant_suffix}-") :])
                    highest_index = max(highest_index, index)
                except ValueError:
                    # Tenant matches prefix but doesn't follow pattern - skip it
                    continue

        start_index = highest_index + 1
        return [
            f"{tenant_suffix}-{i}"
            for i in range(start_index, start_index + number_tenants)
        ]

    def _verify_tenant_creation(
        self,
        collection,
        version: semver.Version,
        new_tenant_names: List[str],
        expected_status: TenantActivityStatus,
    ) -> None:
        """Verify all requested tenants were created with correct status."""
        if version.compare(semver.Version.parse("1.25.0")) < 0:
            created_tenants = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name in new_tenant_names
            }
        else:
            created_tenants = collection.tenants.get_by_names(new_tenant_names)

        if len(created_tenants) != len(new_tenant_names):
            missing = set(new_tenant_names) - set(created_tenants.keys())
            raise Exception(
                f"Not all requested tenants were created. Missing: {', '.join(missing)}"
            )

        for tenant_name, tenant in created_tenants.items():
            if tenant.activity_status != expected_status:
                raise Exception(
                    f"Tenant '{tenant_name}' has status '{tenant.activity_status}', expected '{expected_status}'"
                )

    def delete_tenants(
        self,
        collection: str = DeleteTenantsDefaults.collection,
        tenant_suffix: str = DeleteTenantsDefaults.tenant_suffix,
        number_tenants: int = DeleteTenantsDefaults.number_tenants,
        tenants_list: Optional[list] = DeleteTenantsDefaults.tenants,
    ) -> None:
        """
        Delete tenants for a given collection in Weaviate.

        Args:
            collection (str): The name of the collection to delete tenants from.
            tenant_suffix (str): The suffix of the tenant names to be deleted.
            number_tenants (int): The number of tenants to delete.
            tenants_list (list): A list of tenant names to delete. If not provided, all tenants with the given suffix will be deleted.
        Raises:
            Exception: If the collection does not exist, multi-tenancy is not enabled,
                       no tenants are present, or if there is a failure in deleting tenants.
        """

        version = semver.Version.parse(self.client.get_meta()["version"])
        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command"
            )

        collection = self.client.collections.get(collection)

        if not collection.config.get().multi_tenancy_config.enabled:

            raise Exception(
                f"Collection '{collection.name}' does not have multi-tenancy enabled. Recreate or modify the class with <create class> command"
            )

        if tenant_suffix == "*":
            click.echo(
                f"Deleting {number_tenants} existing tenants from {collection.name} (not taking into account the tenant suffix)"
            )
            tenants_list_with_suffix = collection.tenants.get()
        else:
            if tenants_list is not None:
                # Extract the tenant suffix from the first tenant name in the list
                tenant_suffix = tenants_list[0].split("-")[0]
            tenants_list_with_suffix = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name.startswith(tenant_suffix)
            }
        total_tenants = len(tenants_list_with_suffix)
        try:
            if total_tenants == 0:
                raise Exception(f"No tenants present in class {collection.name}.")
            if tenants_list:
                deleting_tenants = collection.tenants.get_by_names(tenants_list)
            else:

                if number_tenants < total_tenants:
                    # Convert dict to list of items, slice, then convert back to dict
                    deleting_tenants = dict(
                        list(tenants_list_with_suffix.items())[:number_tenants]
                    )
                else:
                    deleting_tenants = tenants_list_with_suffix

            if not deleting_tenants:

                raise Exception(f"No tenants present in class {collection.name}.")
            else:
                for tenant in deleting_tenants.values():
                    collection.tenants.remove(tenant)

        except Exception as e:

            raise Exception(f"Failed to delete tenants: {e}")

        if tenant_suffix == "*":
            remaining_tenants = collection.tenants.get()
        else:
            remaining_tenants = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name.startswith(tenant_suffix)
            }
        removed_tenants = len(deleting_tenants)
        assert (
            len(remaining_tenants) == total_tenants - removed_tenants
        ), f"Expected {total_tenants - removed_tenants} tenants, but found {len(remaining_tenants)}"

        click.echo(f"{len(deleting_tenants)} tenants deleted")

    def get_tenants(
        self,
        collection: str = GetTenantsDefaults.collection,
        tenant_id: str = GetTenantsDefaults.tenant_id,
        verbose: bool = GetTenantsDefaults.verbose,
    ) -> dict:
        """
        Retrieve tenants for a given collection in Weaviate.

        Args:
            collection (str): The name of the collection to retrieve tenants from.
            tenant_id (str): The ID of the tenant to retrieve. If None, retrieves all tenants.
            verbose (bool): If True, prints detailed tenant information. If False, prints summary.

        Returns:
            dict: A dictionary of tenants with tenant names as keys and Tenant objects as values.

        Raises:
            Exception: If the collection does not exist or multi-tenancy is not enabled.
        """
        if tenant_id:
            tenants = self.client.collections.get(collection).tenants.get_by_names(
                [tenant_id]
            )
            if not tenants:
                raise Exception(f"Tenant '{tenant_id}' not found in class {collection}")
        else:
            tenants = self.client.collections.get(collection).tenants.get()

        if verbose:
            click.echo(f"{'Tenant Name':<30}{'Activity Status':<20}")
            for name, tenant in tenants.items():
                click.echo(f"{name:<30}{tenant.activity_status.value:<20}")
        else:
            active_tenants = [
                tenant
                for name, tenant in tenants.items()
                if tenant.activity_status == TenantActivityStatus.ACTIVE
            ]
            inactive_tenants = [
                tenant
                for name, tenant in tenants.items()
                if tenant.activity_status == TenantActivityStatus.INACTIVE
            ]
            offoaded_tenants = [
                tenant
                for name, tenant in tenants.items()
                if tenant.activity_status == TenantActivityStatus.OFFLOADED
            ]

            click.echo(
                f"{'Number Tenants':<20}{'Cold Tenants':<20}{'Hot Tenants': <20}{'Offloaded Tenants':<20}"
            )
            click.echo(
                f"{len(tenants):<20}{len(inactive_tenants):<20}{len(active_tenants):<20}{len(offoaded_tenants):<20}"
            )
        return tenants

    def update_tenants(
        self,
        collection: str = UpdateTenantsDefaults.collection,
        tenant_suffix: str = UpdateTenantsDefaults.tenant_suffix,
        number_tenants: int = UpdateTenantsDefaults.number_tenants,
        state: str = UpdateTenantsDefaults.state,
        tenants: Optional[str] = UpdateTenantsDefaults.tenants,
    ) -> None:
        """
        Updates the activity status of a specified number of tenants in a collection.

        Args:
            collection (str): The name of the collection to update tenants in.
            tenant_suffix (str): The suffix used to filter tenants by name.
            number_tenants (int): The number of tenants to update.
            state (str): The desired activity status for the tenants.
                 Must be one of "hot", "active", "cold", "inactive", "frozen", or "offloaded".

        Raises:
            Exception: If the collection does not exist.
            Exception: If the collection does not have multi-tenancy enabled.
            Exception: If there are not enough tenants with the specified suffix.
            Exception: If a tenant's activity status does not match the expected status after update.

        Returns:
            None
        """

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using ./create_class.py"
            )

        version = semver.Version.parse(self.client.get_meta()["version"])
        collection = self.client.collections.get(collection)

        if not collection.config.get().multi_tenancy_config.enabled:

            raise Exception(
                f"Collection '{collection.name}' does not have multi-tenancy enabled. Recreate or modify the class with ./create_class.py"
            )

        tenant_state_map = {
            "hot": TenantActivityStatus.HOT,
            "active": TenantActivityStatus.ACTIVE,
            "cold": TenantActivityStatus.COLD,
            "inactive": TenantActivityStatus.INACTIVE,
            "frozen": TenantActivityStatus.FROZEN,
            "offloaded": TenantActivityStatus.OFFLOADED,
        }

        equivalent_state_map = {
            "hot": TenantActivityStatus.ACTIVE,
            "active": TenantActivityStatus.ACTIVE,
            "cold": TenantActivityStatus.INACTIVE,
            "inactive": TenantActivityStatus.INACTIVE,
            "frozen": TenantActivityStatus.OFFLOADED,
            "offloaded": TenantActivityStatus.OFFLOADED,
        }

        if tenants is None:
            tenants_with_suffix = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name.startswith(tenant_suffix)
            }

        if tenants is None and len(tenants_with_suffix) < number_tenants:

            raise Exception(
                f"Not enough tenants present in class {collection.name} with suffix {tenant_suffix}. Expected {number_tenants}, found {len(tenants_with_suffix)}."
            )
        if tenants:
            # Update only the specified tenants passed via arguments.
            existing_tenants = collection.tenants.get_by_names(tenants.split(","))
        else:
            existing_tenants = dict(list(tenants_with_suffix.items())[:number_tenants])

        for name, tenant in existing_tenants.items():
            collection.tenants.update(
                Tenant(name=name, activity_status=tenant_state_map[state])
                if tenant.activity_status != tenant_state_map[state]
                else tenant
            )

        # get_by_names is only available after 1.25.0
        if version.compare(semver.Version.parse("1.25.0")) < 0:
            tenants_list = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name in existing_tenants.keys()
            }
        else:
            tenants_list = collection.tenants.get_by_names(
                [name for name in existing_tenants.keys()]
            )
        if tenants is None:
            assert (
                len(tenants_list) == number_tenants
            ), f"Expected {number_tenants} tenants, but found {len(tenants_list)}"
        else:
            assert len(tenants_list) == len(
                tenants.split(",")
            ), f"Expected {len(tenants.split(','))} tenants, but found {len(tenants_list)}"
        for tenant in tenants_list.values():
            if tenant.activity_status != equivalent_state_map[state]:

                raise Exception(
                    f"Tenant '{tenant.name}' has activity status '{tenant.activity_status}', but expected '{tenant_state_map[state]}'"
                )
        click.echo(
            f"{len(tenants_list)} tenants updated with tenant status '{tenant.activity_status}' for class '{collection.name}'."
        )
