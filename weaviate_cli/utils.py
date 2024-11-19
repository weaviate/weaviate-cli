"""
Utility functions.
"""

from collections.abc import Sequence
import string
import random
import weaviate
from weaviate.rbac.models import Permissions, PermissionsCreateType
from typing import Union, List


def get_client_from_context(ctx) -> weaviate.Client:
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
    Format: action:[collection/role/verbosity]

    Supports:
    - CRUD shorthand: crud_collections:Movies
    - Partial CRUD: cr_collections:Movies (create+read)
    - Role permissions: manage_roles, read_roles
    - Cluster permissions: read_cluster
    - Backup permissions: manage_backups
    - Collections permissions: create_collections, read_collections, update_collections, delete_collections
    - Data permissions: create_data, read_data, update_data, delete_data
    - Nodes permissions: read_nodes
    Args:
        perm (str): Permission string

    Returns:
        PermissionsCreateType: Single permission or list for crud/partial crud
    """
    valid_resources = [
        "collections",
        "data",
        "roles",
        # "users", will be added in next release
        "cluster",
        "backups",
        "nodes",
    ]
    parts = perm.split(":")
    if len(parts) > 3 or (parts[0] != "read_nodes" and len(parts) > 2):
        raise ValueError(
            f"Invalid permission format: {perm}. Expected format: action:collection/role/verbosity. Example: manage_roles:custom, crud_collections:Movies, read_nodes:verbose:Movies"
        )
    action = parts[0]
    role = parts[1] if len(parts) > 1 and "roles" in action else "*"
    verbosity = parts[1] if len(parts) > 1 and action == "read_nodes" else "minimal"
    if action == "read_nodes":
        # For read_nodes the first part belongs to the verbosity
        # and the second (optional) belongs to the collection
        # Example: read_nodes:verbose:Movies
        collection = parts[2] if len(parts) > 2 else "*"
    else:
        collection = (
            parts[1]
            if len(parts) > 1
            and action not in ["manage_roles", "read_roles", "read_cluster"]
            else "*"
        )

    # Split collections and roles by comma if they exist
    collection = collection.split(",") if collection != "*" else ["*"]
    role = role.split(",") if role != "*" else ["*"]

    # Handle standalone permissions first
    if action in ["read_cluster", "manage_backups", "read_nodes"]:
        return _create_permission(
            action=action.split("_")[0],
            resource=action.split("_")[1],
            collection=collection,
            verbosity=verbosity,
        )

    # Handle crud and partial crud cases
    if "_" in action:
        parts = action.split("_", 2)
        prefix = parts[0]
        resource = parts[1] if len(parts) > 1 else None
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

        if prefix in ["create", "read", "update", "delete", "manage"]:
            actions = [prefix]
        else:
            # Handle partial crud (curd, cr, ru, ud, etc)
            if not all(c in action_map for c in prefix):
                raise ValueError(f"Invalid crud combination: {prefix}")
            actions = [action_map[c] for c in prefix]

        return _create_permission(
            action=actions,
            resource=resource,
            role=role,
            collection=collection,
            verbosity=verbosity,
        )

    raise ValueError(f"Invalid permission action: {action}")


def _create_permission(
    action: Union[str, Sequence[str]],
    resource: str,
    role: Union[str, Sequence[str]] = "*",
    collection: Union[str, Sequence[str]] = "*",
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
        return Permissions.nodes(read=True, verbosity=verbosity, collection=collection)
    # Handle roles permissions
    elif resource == "roles":
        # will be either "manage" or "read"
        return Permissions.roles(
            role=role, read=("read" in action), manage=("manage" in action)
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

    raise ValueError(f"Invalid permission action: {action}.")
