import json
from typing import List, Optional, Dict, Union
import click
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
        """Get a user in Weaviate. If no user name is provided, the current user is returned.

        Only DB users can be looked up by name: the Weaviate Python client
        exposes ``users.db.get(...)`` but no equivalent ``users.oidc.get(...)``.
        When the DB lookup returns ``None`` (404), the user may still exist
        as an OIDC user — surface that possibility in the error message so
        callers don't waste time chasing a "not found" that is in fact a DB
        vs OIDC mismatch.
        """

        try:
            if user_name is None:
                return self.client.users.get_my_user()
            user = self.client.users.db.get(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error getting user '{user_name}': {e}")
        if user is None:
            raise Exception(
                f"User '{user_name}' not found as a DB user. "
                f"If '{user_name}' is an OIDC user, the Weaviate Python "
                "client does not support fetching OIDC users directly — use "
                f"`get role --user_name {user_name} --user_type oidc` to "
                "list their assigned roles instead."
            )
        return user

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

        Args:
            user_name: The id of the new user. On namespace-enabled clusters
                (Weaviate 1.38.0+) bind the user to a namespace by passing a
                namespace-qualified id of the form ``<namespace>:<user>``.

        Returns:
            The api key for the user.
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

        Returns the api key for the user if the api key was rotated, otherwise
        returns ``None``. Raises if ``activate``/``deactivate`` is a no-op
        because the user is already in the requested state (the underlying
        client returns ``False`` instead of raising on 409).
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
                if not self.client.users.db.activate(user_id=user_name):
                    raise Exception(f"User '{user_name}' is already active.")
                return None
            if deactivate:
                if not self.client.users.db.deactivate(user_id=user_name):
                    raise Exception(f"User '{user_name}' is already deactivated.")
                return None
        except Exception as e:
            raise Exception(f"Error updating user '{user_name}': {e}")

    def delete_user(
        self,
        user_name: Optional[str] = None,
    ) -> None:
        """Delete a user in Weaviate.

        Raises if the user does not exist (the underlying client returns
        ``False`` on a 404 instead of raising).
        """
        if user_name is None:
            raise Exception("User name is required.")
        try:
            deleted = self.client.users.db.delete(user_id=user_name)
        except Exception as e:
            raise Exception(f"Error deleting user '{user_name}': {e}")
        if not deleted:
            raise Exception(f"User '{user_name}' not found.")

    def add_role(
        self,
        role_name: tuple[str],
        user_name: str,
        user_type: str = "db",
        json_output: bool = False,
    ) -> None:
        """Assign a role to a user."""
        try:
            if older_than_version(self.client, "1.30.0"):
                self.client.users.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "db":
                self.client.users.db.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "oidc":
                self.client.users.oidc.assign_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Role(s) {list(role_name)} assigned to {user_type} user '{user_name}' successfully.",
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(
                    f"Role(s) {list(role_name)} assigned to {user_type} user '{user_name}' successfully."
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
        json_output: bool = False,
    ) -> None:
        """Revoke a role from a user."""
        try:
            if older_than_version(self.client, "1.30.0"):
                self.client.users.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "db":
                self.client.users.db.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            elif user_type == "oidc":
                self.client.users.oidc.revoke_roles(
                    user_id=user_name, role_names=list(role_name)
                )
            if json_output:
                click.echo(
                    json.dumps(
                        {
                            "status": "success",
                            "message": f"Role(s) {list(role_name)} revoked from {user_type} user '{user_name}' successfully.",
                        },
                        indent=2,
                    )
                )
            else:
                click.echo(
                    f"Role(s) {list(role_name)} revoked from {user_type} user '{user_name}' successfully."
                )
        except Exception as e:
            raise Exception(
                f"Error revoking {user_type} role '{role_name}' from user '{user_name}': {e}"
            )

    def print_user(self, user: str, json_output: bool = False) -> None:
        """Print user roles in a human readable format."""
        if json_output:
            click.echo(json.dumps({"user_id": user}, indent=2))
            return
        print(f"User: {user}")

    def print_own_user(self, user: OwnUser, json_output: bool = False) -> None:
        """Print user roles in a human readable format."""
        if json_output:
            click.echo(
                json.dumps(
                    {"user_id": user.user_id, "roles": list(user.roles.keys())},
                    indent=2,
                )
            )
            return
        print(f"User: {user.user_id}")
        print(f"Roles:")
        if len(user.roles) == 0:
            print(f" - No roles assigned")
        else:
            for role in user.roles.keys():
                print(f" - Role: {role}")

    def print_db_user(self, user: UserDB, json_output: bool = False) -> None:
        """Print user roles in a human readable format."""
        namespace = getattr(user, "namespace", None)
        if json_output:
            payload = {
                "user_id": user.user_id,
                "active": user.active,
                "user_type": user.user_type.name,
                "roles": list(user.role_names),
            }
            if namespace is not None:
                payload["namespace"] = namespace
            click.echo(json.dumps(payload, indent=2))
            return
        print(f"User: {user.user_id}")
        print(f"Active: {'Yes' if user.active else 'No'}")
        print(f"Type: {user.user_type.name}")
        if namespace is not None:
            print(f"Namespace: {namespace}")
        print(f"Roles:")
        if len(user.role_names) == 0:
            print(f" - No roles assigned")
        else:
            for role in user.role_names:
                print(f" - Role: {role}")
