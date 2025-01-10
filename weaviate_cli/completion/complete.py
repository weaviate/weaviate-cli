import os
import sys

import click
from click.parser import split_arg_string

from weaviate_cli.managers.collection_manager import CollectionManager
from weaviate_cli.managers.config_manager import ConfigManager
from weaviate_cli.managers.role_manager import RoleManager
from weaviate_cli.utils import get_client_from_context


def get_config_parameter(ctx, args, incomplete):
    args = split_arg_string(os.environ["COMP_WORDS"])
    config_file = None
    if "--config-file" in args:
        idx = args.index("--config-file")
        config_file = args[idx + 1]

    return os.path.expanduser(config_file)


def get_user_parameter(ctx, args, incomplete):
    args = split_arg_string(os.environ["COMP_WORDS"])
    user = None
    if "--user" in args:
        idx = args.index("--user")
        user = args[idx + 1]

    return user


def role_name_complete(ctx: click.Context, param: str, incomplete: str) -> [str]:
    config = get_config_parameter(ctx, param, incomplete)
    user = get_user_parameter(ctx, param, incomplete)
    ctx.obj = {"config": ConfigManager(config, user)}
    client = get_client_from_context(ctx)
    role_man = RoleManager(client)
    roles = role_man.get_all_roles().values()
    role_names = []

    for role in roles:
        if incomplete == "" or (incomplete.lower() in role.name.lower()):
            role_names.append(role.name)

    client.close()

    return role_names


def collection_name_complete(ctx: click.Context, param: str, incomplete: str) -> [str]:
    config = get_config_parameter(ctx, param, incomplete)
    user = get_user_parameter(ctx, param, incomplete)
    ctx.obj = {"config": ConfigManager(config, user)}
    client = get_client_from_context(ctx)
    collection_manager = CollectionManager(client)
    collections = collection_manager.get_all_collections()
    collection_names = []

    for collection in collections:
        if incomplete == "" or (incomplete.lower() in collection.lower()):
            collection_names.append(collection)

    client.close()

    return collection_names
