import json
import click

from semi.utils import get_config_from_context
from weaviate.classification import Classification


@click.group('classify', help='Classification of data.')
def classification_group():
    pass


@classification_group.command('get')
@click.pass_context
@click.argument('classification_id')
def get_classification(ctx, classification_id):
    """Get a classification info by id."""
    show_classification_info(ctx, classification_id)
    

@classification_group.command('status')
@click.pass_context
@click.argument('classification_id')
def get_classification_status(ctx, classification_id):
    """Get a classification status by id."""
    show_classification_status(ctx, classification_id)


# TODO: Accept array input
@classification_group.command('start')
@click.pass_context
@click.option('--class-name', required = True, type = str, help = "Object type to classify")
@click.option('--based-on', required = True, type = str, help = "Properties to base classification on")
@click.option('--property', required = True, type = str, help = "Properties to classify")
@click.option('-k', '--k', required = False, type = int, help = "Use kNN classification with value k")
def start_classification(ctx, class_name, based_on, property, k):
    """Start a classification."""
    if k:
        start_contextionary_classification(ctx, class_name, [based_on], [property], k)
    else:
        start_contextionary_classification(ctx, class_name, [based_on], [property])


########################################################################################################################
# Helper functions
########################################################################################################################


def show_classification_status(ctx : click.Context, classification_id : str):
    classification = get_classification_from_ctx(ctx)
    if classification.is_running(classification_id):
        click.echo("Classification is running.")
    elif classification.is_failed(classification_id):
        click.echo("Classification failed.")
    elif classification.is_complete(classification_id):
        click.echo("Classification is complete.")


def show_classification_info(ctx : click.Context, classification_id : str):
    classification = get_classification_from_ctx(ctx)
    result = classification.get(classification_id)
    click.echo(json.dumps(result, indent=2))


def start_contextionary_classification(ctx : click.Context, class_name : str, based_on : list, properties : list):
    classification = get_classification_from_ctx(ctx)
    result = classification.schedule()\
        .with_type("text2vec-contextionary-contextual")\
        .with_class_name(class_name)\
        .with_based_on_properties(based_on)\
        .with_classify_properties(properties)\
        .do()
    show_classification_info(ctx, result['id'])


def start_knn_classification(ctx : click.Context, class_name : str, based_on : list, properties : list, k : int):
    classification = get_classification_from_ctx(ctx)
    result = classification.schedule()\
        .with_type("knn")\
        .with_class_name(class_name)\
        .with_based_on_properties(based_on)\
        .with_classify_properties(properties)\
        .with_settings({'k':k})\
        .do()
    show_classification_info(ctx, result['id'])


def get_classification_from_ctx(ctx : click.Context):
    return get_config_from_context(ctx).client.classification
