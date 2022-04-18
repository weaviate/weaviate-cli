import click
from semi.config.configuration import Configuration
from semi.commands.schema import import_schema, export_schema, delete_schema
from semi.commands.misc import ping, version
from semi.commands.data import delete_all_data, import_data_from_file
import semi.config.config_values as cfg_vals


class NotRequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.not_required_if = kwargs.pop('not_required_if')
        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs['help'] = (kwargs.get('help', '') +
                          ' NOTE: This argument is mutually exclusive with %s' %
                          self.not_required_if).strip()
        super(NotRequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.not_required_if in opts

        if other_present:
            if we_are_present:
                raise click.UsageError(
                    "Illegal usage: `%s` is mutually exclusive with `%s`" % (
                        self.name, self.not_required_if))
            else:
                self.prompt = None

        return super(NotRequiredIf, self).handle_parse_result(
            ctx, opts, args)


@click.group()
def main():
    pass


@main.group("schema", help="Importing and exporting schema files.")
@click.pass_context
def schema_group(ctx: click.Context):
    ctx.obj = {
        "config": Configuration()
    }


@main.group("config", help="Configuration of the CLI.")
@click.pass_context
def config_group(ctx: click.Context):
    ctx.obj = {
        "config": Configuration()
    }


@main.group("data", help="Data object manipulation in weaviate.")
@click.pass_context
def data_group(ctx: click.Context):
    ctx.obj = {
        "config": Configuration()
    }


@main.command('version', help="Print the version of the CLI.")
def main_version():
    version()


@main.command("ping", help="Check if the configured weaviate is reachable.")
def main_ping():
    ping(Configuration())


@main.command(help="Initialize a new weaviate instance configuration.")
@click.option('--url', type=str, help='Weaviate URL', required=True)
@click.option('--user', type=str, help='Weaviate user', cls=NotRequiredIf,
              not_required_if='secret')
@click.option('--password', type=str, help='Weaviate password', cls=NotRequiredIf,
              not_required_if='secret')
@click.option('--secret', type=str, help='Weaviate secret')
def init(url, user, password, secret):
    print("Initializing configuration")
    cfg = {'url': url, 'auth': None}
    if not user and not password and not secret:
        print('No authentication provided')
    elif secret:
        cfg['auth'] = {'type': cfg_vals.config_value_auth_type_client_secret,
                       'secret': secret}
        print('Using client secret authentication')
    else:
        cfg['auth'] = {'type': cfg_vals.config_value_auth_type_username_pass,
                       'user': user,
                       'pass': password}
        print('Using basic authentication')
    print(cfg)


@schema_group.command("import", help="Import a weaviate schema from a json file.")
@click.pass_context
@click.argument('filename')
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


@config_group.command("view", help="Print the current CLI configuration.")
@click.pass_context
def config_view(ctx):
    print(ctx.obj["config"])


@config_group.command("set", help="Set a new CLI configuration.")
@click.pass_context
def config_set(ctx):
    _get_config_from_context(ctx).init()


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


def _get_config_from_context(ctx):
    """

    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"]


if __name__ == '__main__':
    main()
