"""
Utility functions.
"""

import string
import random
import weaviate


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
