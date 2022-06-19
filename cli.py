import click
from semi.config.configuration import Configuration
from semi.commands.schema import import_schema, export_schema, delete_schema
from semi.commands.misc import ping, version
from semi.commands.data import delete_all_data, import_data_from_file
from semi.commands.wcs import create_new_wcs_account, get_wcs_client


@click.group()
@click.pass_context
@click.option('--config-file', required=False, default=None, type=str, is_flag=False,
              help="If specified cli uses the config specified with this path.")
def main(ctx: click.Context, config_file):
    ctx.obj = {
        "config": Configuration(config_file)
    }


# First order commands
@main.group("schema", help="Importing and exporting schema files.")
def schema_group():
    pass


@main.group("config", help="Configuration of the CLI.")
def config_group():
    pass


@main.group("data", help="Data object manipulation in weaviate.")
def data_group():
    pass

@main.group("cloud", help="Manage WCS cluster instances.")
@click.pass_context
def cloud_group(ctx: click.Context):
    ctx.obj["cloud_client"] = get_wcs_client()


@main.command("ping", help="Check if the configured weaviate is reachable.")
@click.pass_context
def main_ping(ctx):
    ping(_get_config_from_context(ctx))


@main.command("version", help="Version of the CLI")
def main_version():
    version()


@main.command("init", help="Initialize a new CLI configuration.")
@click.pass_context
def config_set(ctx):
    _get_config_from_context(ctx).init()


# schema
@schema_group.command("import", help="Import a weaviate schema from a json file.")
@click.pass_context
@click.argument('filename')
#@click.option('--force', required=False, default=False, type=bool, nargs=0)
@click.option('--force', required=False, default=False, is_flag=True)
def schema_import(ctx, filename, force):
    import_schema(_get_config_from_context(ctx), filename, force)


@schema_group.command("export", help="Export a weaviate schema to into a json file.")
@click.pass_context
@click.argument('filename')
def schema_export(ctx, filename):
    export_schema(_get_config_from_context(ctx), filename)


@schema_group.command("delete", help="Delete the entire schema and all the data associated with it.")
@click.pass_context
@click.option('--force', required=False, default=False, is_flag=True)
def schema_truncate(ctx: click.Context, force):
    delete_schema(_get_config_from_context(ctx), force)



# config
@config_group.command("view", help="Print the current CLI configuration.")
@click.pass_context
def config_view(ctx):
    print(ctx.obj["config"])


@config_group.command("set", help="Set a new CLI configuration.")
@click.pass_context
def config_set(ctx):
    _get_config_from_context(ctx).init()

# data
@data_group.command("import", help="Import data from json file.")
@click.pass_context
@click.argument('file')
@click.option('--fail-on-error', required=False, default=False, is_flag=True, help="Fail if entity loading throws an error")
def concept_import(ctx, file, fail_on_error):
    import_data_from_file(_get_config_from_context(ctx), file, fail_on_error)


@data_group.command("delete", help="Delete all data objects in weaviate.")
@click.pass_context
@click.option('--force', required=False, default=False, is_flag=True)
def data_empty(ctx, force):
    delete_all_data(_get_config_from_context(ctx), force)


@cloud_group.command("add", help="Add a new WCS account.")
def cloud_add():
    create_new_wcs_account()


@cloud_group.command("list", help="List all WCS accounts.")
@click.pass_context
def cloud_list(ctx):
    all_clusters = ctx.obj["cloud_client"].get_clusters()
    if all_clusters:
        print("Available clusters:")
        for idx in range(len(all_clusters)):
            print(f"{idx + 1}. {all_clusters[idx]}")
    else:
        print("No clusters available.")


@cloud_group.command("create", help="Create a new WCS cluster.")
@click.pass_context
@click.option('--name', required=True, type=str, help="Name of the cluster.")
# @click.option('--active', '-a', required=False, default=False, is_flag=True, help="Activate the cluster.")
def cloud_create(ctx, name):
    ctx.obj["cloud_client"].create(name)


@cloud_group.command("delete", help="Delete a WCS cluster.")
@click.pass_context
@click.argument('cluster_id')
def cloud_delete(ctx, cluster_id):
    ctx.obj["cloud_client"].delete_cluster(cluster_id)


@cloud_group.command("status", help="Get the status of a WCS cluster.")
@click.pass_context
@click.argument('cluster_id')
def cloud_status(ctx, cluster_id):
    if ctx.obj["cloud_client"].is_ready(cluster_id):
        print("Ready.")
    else:
        print("Not ready.")


def _get_config_from_context(ctx):
    """

    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"]


if __name__ == "__main__":
    main()
