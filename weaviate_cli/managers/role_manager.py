from typing import Optional, List, Dict
import json
from weaviate_cli.utils import parse_permission
from weaviate import WeaviateClient
from weaviate.rbac.models import Role
from weaviate_cli.defaults import (
    CreateRoleDefaults,
    DeleteRoleDefaults,
    GetRoleDefaults,
)


class RoleManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def create_role(
        self,
        role_name: str = CreateRoleDefaults.role_name,
        permissions: tuple = CreateRoleDefaults.permission,
    ) -> None:
        try:
            rbac_permissions = []
            for perm in permissions:
                parsed = parse_permission(perm)
                if isinstance(parsed, list):
                    rbac_permissions.extend(parsed)
                else:
                    rbac_permissions.append(parsed)

            self.client.roles.create(role_name=role_name, permissions=rbac_permissions)

        except Exception as e:
            raise Exception(f"Error creating role '{role_name}': {e}")

    def get_role(self, role_name: str) -> Optional[Role]:
        try:
            return self.client.roles.by_name(role_name=role_name)
        except Exception as e:
            raise Exception(f"Error getting role '{role_name}': {e}")

    def get_roles_from_user(self, user_name: str) -> Dict[str, Role]:
        try:
            return self.client.roles.by_user(user=user_name)
        except Exception as e:
            raise Exception(f"Error getting roles from user '{user_name}': {e}")

    def delete_role(self, role_name: str = DeleteRoleDefaults.role_name) -> None:
        try:
            if not self.client.roles.exists(role_name=role_name):
                raise Exception(f"Role '{role_name}' does not exist.")
            self.client.roles.delete(role_name=role_name)
            assert not self.client.roles.exists(role_name=role_name)
        except Exception as e:
            raise Exception(f"Error deleting role '{role_name}': {e}")

    def get_all_roles(self) -> Dict[str, Role]:
        try:
            return self.client.roles.list_all()
        except Exception as e:
            raise Exception(f"Error getting all roles: {e}")

    def role_of_current_user(self) -> Dict[str, Role]:
        """Get the role of the current user."""
        try:
            return self.client.roles.of_current_user()
        except Exception as e:
            raise Exception(f"Error getting role of current user: {e}")

    def add_permission(self, permission: str, role_name: str) -> None:
        try:
            rbac_permissions = []
            for perm in permission:
                parsed = parse_permission(perm)
                rbac_permissions = rbac_permissions + parsed
            self.client.roles.add_permissions(
                permissions=rbac_permissions, role_name=role_name
            )
        except Exception as e:
            raise Exception(
                f"Error adding permission '{permission}' to role '{role_name}': {e}"
            )

    def revoke_permission(self, permission: str, role_name: str) -> None:
        try:
            rbac_permissions = []
            for perm in permission:
                parsed = parse_permission(perm)
                rbac_permissions = rbac_permissions + parsed
            self.client.roles.remove_permissions(
                permissions=rbac_permissions, role_name=role_name
            )
        except Exception as e:
            raise Exception(
                f"Error revoking permission '{permission}' from role '{role_name}': {e}"
            )

    def print_role(self, role: Optional[Role] = None) -> None:
        """Print a role and its permissions in a human readable format."""
        if not role:
            raise ValueError("The role does not exist.")

        separator = "-" * 50
        print(f"\n{separator}")
        print(f"Role: {role.name}")

        if role.cluster_permissions:
            print("\nCluster Actions:")
            for perm in role.cluster_permissions:
                print(f"  - {perm.action.value}")

        if role.nodes_permissions:
            print("\nNodes Permissions:")
            for perm in role.nodes_permissions:
                print(
                    f"  - Verbosity: {perm.verbosity}, Collection: {perm.collection if perm.collection else '*'}, Action: {perm.action.value}"
                )

        if role.backups_permissions:
            print("\nBackups Permissions:")
            for perm in role.backups_permissions:
                print(f"  - Collection: {perm.collection}, Action: {perm.action.value}")

        if role.roles_permissions:
            print("\nRoles Permissions:")
            for perm in role.roles_permissions:
                print(f"  - Role: {perm.role}, Action: {perm.action.value}")

        if role.collections_permissions:
            print("\nCollections (schema) Permissions:")
            for perm in role.collections_permissions:
                print(
                    f"  - Collection: {perm.collection}, Tenant: *, Action: {perm.action.value}"
                )

        if role.data_permissions:
            print("\nData Permissions:")
            for perm in role.data_permissions:
                print(f"  - Collection: {perm.collection}, Action: {perm.action.value}")
