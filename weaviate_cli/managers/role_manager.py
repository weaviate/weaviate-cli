from typing import Optional, List, Dict
import json
from weaviate_cli.utils import parse_permission
from weaviate import WeaviateClient
from weaviate.rbac.models import Role, RBAC
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
        name: str = CreateRoleDefaults.name,
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

            self.client.roles.create(name=name, permissions=rbac_permissions)

        except Exception as e:
            raise Exception(f"Error creating role '{name}': {e}")

    def get_role(self, name: str) -> Role:
        try:
            return self.client.roles.by_name(name)
        except Exception as e:
            raise Exception(f"Error getting role '{name}': {e}")

    def get_roles_from_user(self, for_user: str) -> Dict[str, Role]:
        try:
            return self.client.roles.by_user(user=for_user)
        except Exception as e:
            raise Exception(f"Error getting roles from user '{for_user}': {e}")

    def delete_role(self, name: str = DeleteRoleDefaults.name) -> None:
        try:
            if not self.client.roles.exists(name):
                raise Exception(f"Role '{name}' does not exist.")
            self.client.roles.delete(name)
            assert not self.client.roles.exists(name)
        except Exception as e:
            raise Exception(f"Error deleting role '{name}': {e}")

    def get_all_roles(self) -> List[Role]:
        try:
            return self.client.roles.list_all()
        except Exception as e:
            raise Exception(f"Error getting all roles: {e}")

    def add_permission(self, permission: str, to_role: str) -> None:
        try:
            rbac_permissions = []
            for perm in permission:
                parsed = parse_permission(perm)
                rbac_permissions = rbac_permissions + parsed
            self.client.roles.add_permissions(
                permissions=rbac_permissions, role=to_role
            )
        except Exception as e:
            raise Exception(
                f"Error adding permission '{permission}' to role '{to_role}': {e}"
            )

    def revoke_permission(self, permission: str, from_role: str) -> None:
        try:
            rbac_permissions = []
            for perm in permission:
                parsed = parse_permission(perm)
                rbac_permissions = rbac_permissions + parsed
            self.client.roles.remove_permissions(
                permissions=rbac_permissions, role=from_role
            )
        except Exception as e:
            raise Exception(
                f"Error revoking permission '{permission}' from role '{from_role}': {e}"
            )

    def print_role(self, role: Optional[Role] = None) -> None:
        """Print a role and its permissions in a human readable format."""
        if not role:
            raise ValueError("Role is required. Please provide a valid role.")

        separator = "-" * 50
        print(f"\n{separator}")
        print(f"Role: {role.name}")

        if role.cluster_actions:
            print("\nCluster Actions:")
            for action in role.cluster_actions:
                print(f"  - {action.value}")

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

        if role.config_permissions:
            print("\nCollections (schema) Permissions:")
            for perm in role.config_permissions:
                print(
                    f"  - Collection: {perm.collection}, Tenant: {perm.tenant}, Action: {perm.action.value}"
                )

        if role.data_permissions:
            print("\nData Permissions:")
            for perm in role.data_permissions:
                print(f"  - Collection: {perm.collection}, Action: {perm.action.value}")
