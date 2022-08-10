"""
Utility functions.
"""

import click
import weaviate


def get_client_from_context(ctx) -> weaviate.Client:
    """
        Get Configuration object from the specified file.
    :param ctx:
    :return:
    :rtype: semi.config.configuration.Configuration
    """
    return ctx.obj["config"].get_client()


class Mutex(click.Option):
    """
    Class to implement Not Required If in Click.
    """

    def __init__(self, *args, **kwargs):
        self.not_required_if:list = kwargs.pop("not_required_if")

        assert self.not_required_if, "'not_required_if' parameter required"
        kwargs["help"] = (kwargs.get("help", "") + "Option is mutually exclusive with, "
                            + "".join(self.not_required_if) + ".")
        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        for mutex_opt in self.not_required_if:
            if mutex_opt in opts:
                if self.name in opts:
                    raise click.UsageError("Illegal usage: '" + str(self.name)
                                           + "' is mutually exclusive with " + str(mutex_opt) + ".")
                self.prompt = None
        return super().handle_parse_result(ctx, opts, args)
