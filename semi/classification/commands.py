"""
Weaviate CLI classification group functions.
"""
import json
import click

from semi.utils import get_client_from_context


@click.group('classify')
@click.pass_context
def classification_group(ctx : click.Context):
    """
        Classification of data.
    """

    ctx.obj["classification"] = get_classification_from_ctx(ctx)


@classification_group.command('get')
@click.pass_context
@click.argument('classification_id')
def get_classification(ctx, classification_id):
    """
    Get a classification info by id.
    """

    show_classification_info(ctx.obj["classification"], classification_id)


@classification_group.command('status')
@click.pass_context
@click.argument('classification_id')
def get_classification_status(ctx, classification_id):
    """
    Get a classification status by id.
    """

    show_classification_status(ctx.obj["classification"], classification_id)


# TODO: Accept array input
@classification_group.command('start')
@click.pass_context
@click.option('--class-name', required = True, type = str,
                                help = "Object type to classify")
@click.option('--based-on', required = True, type = str,
                                help = "Properties to base classification on")
@click.option('--property', required = True, type = str,
                                help = "Properties to classify")
@click.option('-k', '--k', required = False, type = int,
                                help = "Use kNN classification with value k")
def start_classification(ctx, class_name, based_on, property, k):
    """
    Start a classification.
    """

    if k:
        start_knn_classification(ctx.obj["classification"], class_name,
                                                            [based_on], [property], k)
    else:
        start_contextionary_classification(ctx.obj["classification"], class_name,
                                                                [based_on], [property])


####################################################################################################
# Helper functions
####################################################################################################


def show_classification_status(classification, classification_id : str):
    """
    Get a classification status by id.

    Parameters
    ----------
    classification : Classification
        Classification client.
    classification_id : str
        Classification id.
    """

    if classification.is_running(classification_id):
        click.echo("Classification is running.")
    elif classification.is_failed(classification_id):
        click.echo("Classification failed.")
    elif classification.is_complete(classification_id):
        click.echo("Classification is complete.")


def show_classification_info(classification, classification_id : str):
    """
    Get a classification info by id.

    Parameters
    ----------
    classification : Classification
        Classification client.
    classification_id : str
        Classification id.
    """

    result = classification.get(classification_id)
    click.echo(json.dumps(result, indent=4))


def start_contextionary_classification(classification, class_name : str, based_on : list, properties : list):
    """
    Start a contextionary classification.

    Parameters
    ----------
    classification : Classification
        Classification client.
    class_name : str
        Object type to classify.
    based_on : list
        Properties to base classification on.
    properties : list
        Properties to classify.
    """

    result = classification.schedule()\
        .with_type("text2vec-contextionary-contextual")\
        .with_class_name(class_name)\
        .with_based_on_properties(based_on)\
        .with_classify_properties(properties)\
        .do()
    show_classification_info(classification, result['id'])


def start_knn_classification(classification, class_name : str, based_on : list, properties : list, k : int):
    """
    Start a kNN classification.

    Parameters
    ----------
    classification : Classification
        Classification client.
    class_name : str
        Object type to classify.
    based_on : list
        Properties to base classification on.
    properties : list
        Properties to classify.
    k : int
        kNN value.
    """

    result = classification.schedule()\
        .with_type("knn")\
        .with_class_name(class_name)\
        .with_based_on_properties(based_on)\
        .with_classify_properties(properties)\
        .with_settings({'k':k})\
        .do()
    show_classification_info(classification, result['id'])


def get_classification_from_ctx(ctx : click.Context):
    return get_client_from_context(ctx).classification
