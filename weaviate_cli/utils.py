"""
Utility functions.
"""

from collections.abc import Sequence
import click
import string
import random
import semver
import weaviate
from weaviate.rbac.models import Permissions, RoleScope, PermissionsCreateType
from typing import Optional, Union, List


def get_client_from_context(ctx: click.Context) -> weaviate.WeaviateClient:
    """
        Get Configuration object from the specified file.
    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"].get_client()


# Insert objects to the replicated collection
def get_random_string(length):
    # choose from all lowercase letter
    letters = string.ascii_lowercase
    result_str = "".join(random.choice(letters) for i in range(length))
    return result_str


# Pretty print objects in the response in a table format
def pp_objects(response, main_properties):

    # Create the header
    header = f"{'ID':<37}"
    for prop in main_properties:
        header += f"{prop.capitalize():<37}"
    header += f"{'Distance':<11}{'Certainty':<11}{'Score':<11}"
    print(header)

    objects = []
    if type(response) == weaviate.collections.classes.internal.ObjectSingleReturn:
        objects.append(response)
    else:
        objects = response.objects

    if len(objects) == 0:
        print("No objects found")
        return

    # Print each object
    for obj in objects:
        row = f"{str(obj.uuid):<36} "
        for prop in main_properties:
            row += f"{str(obj.properties.get(prop, ''))[:36]:<36} "
        row += f"{str(obj.metadata.distance)[:10] if hasattr(obj.metadata, 'distance') else 'None':<10} "
        row += f"{str(obj.metadata.certainty)[:10] if hasattr(obj.metadata, 'certainty') else 'None':<10} "
        row += f"{str(obj.metadata.score)[:10] if hasattr(obj.metadata, 'score') else 'None':<10}"
        print(row)

    # Print footer
    footer = f"{'':<37}" * (len(main_properties) + 1) + f"{'':<11}{'':<11}{'':<11}"
    print(footer)
    print(f"Total: {len(objects)} objects")


def parse_permission(perm: str) -> PermissionsCreateType:
    """
    Convert a permission string to RBAC permission object(s).
    Format: action:[collection/user/role/verbosity]

    Supports:
    - CRUD shorthand: crud_collections:Movies
    - Partial CRUD: cr_collections:Movies (create+read)
    - Role permissions: read_roles, update_roles, delete_roles, create_roles
    - Cluster permissions: read_cluster
    - Backup permissions: manage_backups
    - Collections permissions: create_collections, read_collections, update_collections, delete_collections
    - Tenants permissions: create_tenants, read_tenants, update_tenants, delete_tenants
    - Data permissions: create_data, read_data, update_data, delete_data
    - Users permissions: create_users, read_users, update_users, delete_users, assign_and_revoke_users
    - Nodes permissions: read_nodes
    - Aliases permissions: create_aliases, read_aliases, update_aliases, delete_aliases
    Args:
        perm (str): Permission string

    Returns:
        PermissionsCreateType: Single permission or list for crud/partial crud
    """
    valid_resources = [
        "collections",
        "data",
        "roles",
        "tenants",
        "users",
        "cluster",
        "backups",
        "nodes",
        "aliases",
    ]
    crud_resources = ["collections", "data", "tenants", "roles", "users", "aliases"]
    parts = perm.split(":")
    # Only read_nodes  and *_roles, *_tenants, *_aliases can have 3 parts: read_nodes:verbosity:collection, read_roles:role:scope, read_tenants:collection:tenant, read_aliases:collection:alias
    if len(parts) > 3 or (
        (
            parts[0].split("_")[-1] not in ["roles", "tenants", "aliases"]
            and parts[0] not in ["read_nodes"]
        )
        and len(parts) > 2
    ):
        raise ValueError(
            f"Invalid permission format: {perm}. Expected format: action:collection/role/user/verbosity:scope/tenant/alias. Example: create_roles:custom:all, crud_collections:Movies, crud_aliases:Movies, create_users:admin-user, read_nodes:verbose:Movies, read_tenants:Movies:tenant1, read_aliases:Movies:alias1"
        )
    action = parts[0]
    role = parts[1] if len(parts) > 1 and "roles" in action else "*"
    role_scope = parts[2] if len(parts) > 2 and "roles" in action else None
    tenant = parts[2].split(",") if len(parts) > 2 and "tenants" in action else None
    alias = parts[2].split(",") if len(parts) > 2 and "aliases" in action else "*"
    user = parts[1].split(",") if len(parts) > 1 and "users" in action else "*"

    verbosity = "minimal"
    if action == "read_nodes":
        # Handle read_nodes format: read_nodes:verbosity:collection
        if len(parts) > 1:
            if parts[1] not in ["minimal", "verbose"]:
                raise ValueError("Input should be 'minimal' or 'verbose'")
            verbosity = parts[1] if parts[1] == "verbose" else "minimal"
        collection = parts[2] if len(parts) > 2 else "*"
    else:
        # Handle all other actions
        role_actions = ["update_roles", "delete_roles", "create_roles", "read_roles"]
        collection = (
            parts[1]
            if len(parts) > 1 and action not in role_actions + ["read_cluster"]
            else "*"
        )

    # Split collections and roles by comma if they exist
    collection = collection.split(",") if collection != "*" else ["*"]
    role = role.split(",") if role != "*" else ["*"]

    # Handle standalone permissions first
    if action in [
        "read_cluster",
        "manage_backups",
        "read_nodes",
        "assign_and_revoke_users",
    ]:
        return _create_permission(
            action=(
                action.split("_")[0]
                if action != "assign_and_revoke_users"
                else "assign_and_revoke"
            ),
            resource=action.split("_")[-1],
            user=user,
            collection=collection,
            verbosity=verbosity,
        )

    # Handle crud and partial crud cases
    if "_" in action:
        parts = action.split("_", 2)
        prefix = parts[0]
        resource = parts[1] if len(parts) > 1 else None
        if resource in crud_resources:
            if resource not in valid_resources:
                # Find closest matching resource type
                closest = min(
                    valid_resources,
                    key=lambda x: (
                        sum(c1 != c2 for c1, c2 in zip(x, resource))
                        if len(x) == len(resource)
                        else abs(len(x) - len(resource))
                    ),
                )
                suggestion = f"\nDid you mean '{closest}'?" if closest else ""
                raise ValueError(f"Invalid resource type: {resource}. {suggestion}")

            action_map = {"c": "create", "r": "read", "u": "update", "d": "delete"}

            if prefix in ["create", "read", "update", "delete"]:
                actions = [prefix]
            else:
                # Handle partial crud (curd, cr, ru, ud, etc)
                if not all(c in action_map for c in prefix):
                    raise ValueError(f"Invalid crud combination: {prefix}")
                actions = [action_map[c] for c in prefix]

            return _create_permission(
                action=actions,
                resource=resource,
                collection=collection,
                tenant=tenant,
                alias=alias,
                role=role,
                role_scope=role_scope,
                user=user,
            )

    raise ValueError(f"Invalid permission action: {action}")


def _create_permission(
    action: Union[str, Sequence[str]],
    resource: str,
    role: Union[str, Sequence[str]] = "*",
    role_scope: Optional[str] = None,
    user: Union[str, Sequence[str]] = "*",
    collection: Union[str, Sequence[str]] = "*",
    tenant: Union[str, Sequence[str]] = "*",
    alias: Union[str, Sequence[str]] = "*",
    verbosity: str = "minimal",
) -> PermissionsCreateType:
    """Helper function to create individual RBAC permission objects."""

    # Handle standalone permissions
    # if action == "manage_users":
    #     return Permissions.users(manage=True)
    if resource == "cluster":
        return Permissions.cluster(read=True)
    elif resource == "backups":
        return Permissions.backup(manage=True, collection=collection)
    elif resource == "nodes":
        if verbosity == "minimal":
            if collection != "*":
                print(
                    f"WARNING: Collection is not supported for minimal verbosity. Ignoring collection: {collection}"
                )
            return Permissions.Nodes.minimal(read=True)
        else:
            return Permissions.Nodes.verbose(collection=collection, read=True)
    elif resource == "users":
        return Permissions.users(
            user=user,
            create=("create" in action),
            read=("read" in action),
            update=("update" in action),
            delete=("delete" in action),
            assign_and_revoke=("assign_and_revoke" in action),
        )
    # Handle roles permissions
    elif resource == "roles":
        roles_scope_map = {
            "all": RoleScope.ALL,
            "match": RoleScope.MATCH,
        }
        scope = None
        if role_scope:
            if role_scope not in roles_scope_map.keys():
                raise ValueError(
                    f"Invalid role scope: {role_scope}, expected one of: {', '.join(roles_scope_map.keys())}"
                )
            scope = roles_scope_map[role_scope]

        return Permissions.roles(
            role=role,
            read=("read" in action),
            update=("update" in action),
            delete=("delete" in action),
            create=("create" in action),
            scope=scope,
        )

    # Handle schema permissions
    elif resource == "collections":
        return Permissions.collections(
            collection=collection,
            create_collection=("create" in action),
            read_config=("read" in action),
            update_config=("update" in action),
            delete_collection=("delete" in action),
        )

    # Handle data permissions
    elif resource == "data":
        return Permissions.data(
            collection=collection,
            create=("create" in action),
            read=("read" in action),
            update=("update" in action),
            delete=("delete" in action),
        )

    # Handle tenants permissions
    elif resource == "tenants":
        return Permissions.tenants(
            tenant=tenant,
            collection=collection,
            create=("create" in action),
            read=("read" in action),
            update=("update" in action),
            delete=("delete" in action),
        )
    # Handle aliases permissions
    elif resource == "aliases":
        return Permissions.alias(
            alias=alias,
            collection=collection,
            create=("create" in action),
            read=("read" in action),
            update=("update" in action),
            delete=("delete" in action),
        )
    raise ValueError(f"Invalid permission action: {action}.")


def older_than_version(client, minimum_version):
    client_version = client.get_meta()["version"]
    if "-" in client_version:
        client_version = client_version.split("-")[0]
    return is_version_older_than(client_version, minimum_version)


def is_version_older_than(version, check_version):
    if (
        semver.Version.compare(
            semver.Version.parse(version), semver.Version.parse(check_version)
        )
        < 0
    ):
        return True
    return False
