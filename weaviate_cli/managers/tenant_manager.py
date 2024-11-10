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
        state: str = CreateTenantsDefaults.state,
    ) -> None:
        """
        Create tenants for a given collection in Weaviate.

        Args:
            collection (str): The name of the collection to add tenants to.
            tenant_suffix (str): The suffix to append to tenant names.
            number_tenants (int): The number of tenants to create.
            state (str): The activity status of the tenants to be created.

        Raises:
            Exception: If the collection does not exist, multi-tenancy is not enabled,
                       tenants already exist, or if there is a mismatch in tenant activity status.
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

        existing_tenants = collection.tenants.get()
        if existing_tenants:

            raise Exception(
                f"Tenants already exist in class '{collection.name}'. Update their status using ./update_tenants.py or delete them using <delete tenants> command"
            )
        else:
            collection.tenants.create(
                [
                    Tenant(
                        name=f"{tenant_suffix}{i}",
                        activity_status=tenant_state_map[state],
                    )
                    for i in range(number_tenants)
                ]
            )

        # get_by_names is only available after 1.25.0
        if version.compare(semver.Version.parse("1.25.0")) < 0:
            tenants_list = {
                name: tenant
                for name, tenant in collection.tenants.get().items()
                if name.startswith(tenant_suffix)
            }
        else:
            tenants_list = collection.tenants.get_by_names(
                [f"{tenant_suffix}{i}" for i in range(number_tenants)]
            )
        assert (
            len(tenants_list) == number_tenants
        ), f"Expected {number_tenants} tenants, but found {len(tenants_list)}"
        for tenant in tenants_list.values():
            if tenant.activity_status != tenant_state_map[state]:

                raise Exception(
                    f"Tenant '{tenant.name}' has activity status '{tenant.activity_status}', but expected '{tenant_state_map[state]}'"
                )
        click.echo(
            f"{len(tenants_list)} tenants added with tenant status '{tenant.activity_status}' for collection '{collection.name}'"
        )

    def delete_tenants(
        self,
        collection: str = DeleteTenantsDefaults.collection,
        tenant_suffix: str = DeleteTenantsDefaults.tenant_suffix,
        number_tenants: int = DeleteTenantsDefaults.number_tenants,
    ) -> None:
        """
        Delete tenants for a given collection in Weaviate.

        Args:
            collection (str): The name of the collection to delete tenants from.
            tenant_suffix (str): The suffix of the tenant names to be deleted.
            number_tenants (int): The number of tenants to delete.

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

        total_tenants = len(collection.tenants.get())
        try:
            if total_tenants == 0:

                raise Exception(f"No tenants present in class {collection.name}.")
            # get_by_names is only available after 1.25.0
            if version.compare(semver.Version.parse("1.25.0")) < 0:
                tenants_list = {
                    name: tenant
                    for name, tenant in collection.tenants.get().items()
                    if name.startswith(tenant_suffix)
                }
                deleting_tenants = {
                    name: tenant
                    for name, tenant in tenants_list.items()
                    if int(name[len(tenant_suffix) :]) < number_tenants
                }
            else:
                deleting_tenants = collection.tenants.get_by_names(
                    [
                        f"{tenant_suffix}{i}"
                        for i in range(
                            number_tenants
                            if number_tenants < total_tenants
                            else total_tenants
                        )
                    ]
                )
            if not deleting_tenants:

                raise Exception(f"No tenants present in class {collection.name}.")
            else:
                for name, tenant in deleting_tenants.items():
                    collection.tenants.remove(Tenant(name=name))

        except Exception as e:

            raise Exception(f"Failed to delete tenants: {e}")

        tenants_list = collection.tenants.get()
        assert (
            len(tenants_list) == total_tenants - number_tenants
        ), f"Expected {total_tenants - number_tenants} tenants, but found {len(tenants_list)}"

        click.echo(f"{number_tenants} tenants deleted")

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
            click.echo(f"{'Tenant Name':<20}{'Activity Status':<20}")
            for name, tenant in tenants.items():
                click.echo(f"{name:<20}{tenant.activity_status.value:<20}")
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

        tenants_with_suffix = {
            name: tenant
            for name, tenant in collection.tenants.get().items()
            if name.startswith(tenant_suffix)
        }

        if len(tenants_with_suffix) < number_tenants:

            raise Exception(
                f"Not enough tenants present in class {collection.name} with suffix {tenant_suffix}. Expected {number_tenants}, found {len(tenants_with_suffix)}."
            )

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

        assert (
            len(tenants_list) == number_tenants
        ), f"Expected {number_tenants} tenants, but found {len(tenants_list)}"
        for tenant in tenants_list.values():
            if tenant.activity_status != equivalent_state_map[state]:

                raise Exception(
                    f"Tenant '{tenant.name}' has activity status '{tenant.activity_status}', but expected '{tenant_state_map[state]}'"
                )
        click.echo(
            f"{len(tenants_list)} tenants updated with tenant status '{tenant.activity_status}' for class '{collection.name}'."
        )
