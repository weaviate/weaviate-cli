from typing import List, Optional, Dict
from weaviate import WeaviateClient
from weaviate.rbac.models import User
from weaviate_cli.defaults import (
    GetUserDefaults,
)


class UserManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def get_user_from_role(
        self, role_name: str = GetUserDefaults.role_name
    ) -> List[str]:
        """Get all roles assigned to a user."""
        try:
            return self.client.roles.get_assigned_user_ids(role_name=role_name)
        except Exception as e:
            raise Exception(f"Error getting users for role '{role_name}': {e}")

    def add_role(
        self,
        role_name: tuple[str],
        user_name: str,
    ) -> None:
        """Assign a role to a user."""
        try:
            self.client.users.assign_roles(
                user_id=user_name, role_names=list(role_name)
            )
        except Exception as e:
            raise Exception(
                f"Error assigning role '{role_name}' to user '{user_name}': {e}"
            )

    def revoke_role(
        self,
        role_name: tuple[str],
        user_name: str,
    ) -> None:
        """Revoke a role from a user."""
        try:
            self.client.users.revoke_roles(
                user_id=user_name, role_names=list(role_name)
            )
        except Exception as e:
            raise Exception(
                f"Error revoking role '{role_name}' from user '{user_name}': {e}"
            )

    def print_user(self, user: User) -> None:
        """Print user roles in a human readable format."""
        print(f"User: {user}")
