from typing import List, Optional, Dict, Union
from weaviate import WeaviateClient
from weaviate.users.users import OwnUser, UserDB, UserTypes
from weaviate_cli.defaults import (
    GetUserDefaults,
)
from weaviate_cli.utils import older_than_version


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

    def get_user(
        self,
        user_name: Optional[str] = None,
    ) -> Union[OwnUser, UserDB]:
        """Get a user in Weaviate. If no user name is provided, the current user is returned."""

        try:
            if user_name is None:
                return self.client.users.get_my_user()
            else:
                return self.client.users.db.get(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error getting user '{user_name}': {e}")

    def get_all_users(self) -> List[UserDB]:
        """Get all users in Weaviate."""
        try:
            return self.client.users.db.list_all()
        except Exception as e:
            raise Exception(f"Error getting all users: {e}")

    def create_user(
        self,
        user_name: Optional[str] = None,
    ) -> str:
        """
        Create a user in Weaviate.
        Returns the api key for the user.
        """
        if user_name is None:
            raise Exception("User name is required.")
        try:
            return self.client.users.db.create(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error creating user '{user_name}': {e}")

    def update_user(
        self,
        user_name: Optional[str] = None,
        rotate_api_key: bool = False,
        activate: bool = False,
        deactivate: bool = False,
    ) -> Optional[str]:
        """Update a user in Weaviate.
        Returns the api key for the user if the api key was rotated, otherwise returns None.
        """
        if user_name is None:
            raise Exception("User name is required.")
        if rotate_api_key and (activate or deactivate):
            raise Exception(
                "Cannot rotate api key and activate or deactivate user at the same time."
            )
        if activate and deactivate:
            raise Exception("Cannot activate and deactivate user at the same time.")
        try:
            if rotate_api_key:
                return self.client.users.db.rotate_key(user_id=user_name)
            if activate:
                return self.client.users.db.activate(user_id=user_name)
            if deactivate:
                return self.client.users.db.deactivate(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error updating user '{user_name}': {e}")

    def delete_user(
        self,
        user_name: Optional[str] = None,
    ) -> None:
        """Delete a user in Weaviate."""
        if user_name is None:
            raise Exception("User name is required.")
        try:
            self.client.users.db.delete(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error deleting user '{user_name}': {e}")

    def add_role(
        self,
        role_name: tuple[str],
        user_name: str,
        user_type: str = "db",
    ) -> None:
        """Assign a role to a user."""
        try:
            if older_than_version(self.client, "1.30.0"):
                return self.client.users.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            if user_type == "db":
                self.client.users.db.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "oidc":
                self.client.users.oidc.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
        except Exception as e:
            raise Exception(
                f"Error assigning {user_type} role '{role_name}' to user '{user_name}': {e}"
            )

    def revoke_role(
        self,
        role_name: tuple[str],
        user_name: str,
        user_type: str = "db",
    ) -> None:
        """Revoke a role from a user."""
        try:
            if older_than_version(self.client, "1.30.0"):
                return self.client.users.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            if user_type == "db":
                self.client.users.db.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "oidc":
                self.client.users.oidc.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
        except Exception as e:
            raise Exception(
                f"Error revoking {user_type} role '{role_name}' from user '{user_name}': {e}"
            )

    def print_user(self, user: str) -> None:
        """Print user roles in a human readable format."""
        print(f"User: {user}")

    def print_own_user(self, user: OwnUser) -> None:
        """Print user roles in a human readable format."""
        print(f"User: {user.user_id}")
        print(f"Roles:")
        if len(user.roles) == 0:
            print(f" - No roles assigned")
        else:
            for role in user.roles.keys():
                print(f" - Role: {role}")

    def print_db_user(self, user: UserDB) -> None:
        """Print user roles in a human readable format."""
        print(f"User: {user.user_id}")
        print(f"Active: {'Yes' if user.active else 'No'}")
        print(f"Type: {user.user_type.name}")
        print(f"Roles:")
        if len(user.role_names) == 0:
            print(f" - No roles assigned")
        else:
            for role in user.role_names:
                print(f" - Role: {role}")
