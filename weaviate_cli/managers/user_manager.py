from typing import Optional, Dict
from weaviate import WeaviateClient
from weaviate.rbac.models import User
from weaviate_cli.defaults import (
    GetUserDefaults,
)


class UserManager:
    def __init__(self, client: WeaviateClient):
        self.client = client

    def get_user_from_role(
        self, of_role: str = GetUserDefaults.of_role
    ) -> Dict[str, User]:
        """Get all roles assigned to a user."""
        try:
            return self.client.roles.users(of_role)
        except Exception as e:
            raise Exception(f"Error getting users for role '{of_role}': {e}")

    def add_role(
        self,
        role: tuple[str],
        to_user: str,
    ) -> None:
        """Assign a role to a user."""
        try:
            self.client.roles.assign(roles=list(role), user=to_user)
        except Exception as e:
            raise Exception(f"Error assigning role '{role}' to user '{to_user}': {e}")

    def revoke_role(
        self,
        role: tuple[str],
        from_user: str,
    ) -> None:
        """Revoke a role from a user."""
        try:
            self.client.roles.revoke(roles=list(role), user=from_user)
        except Exception as e:
            raise Exception(
                f"Error revoking role '{role}' from user '{from_user}': {e}"
            )

    def print_user(self, user: User) -> None:
        """Print user roles in a human readable format."""
        print(f"User: {user.name}")
